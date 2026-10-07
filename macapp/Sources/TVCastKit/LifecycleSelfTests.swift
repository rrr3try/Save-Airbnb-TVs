import Foundation

/// Loopback-only regressions: no television, screen recording, or encoder required.
public func runLifecycleSelfTests() -> Int {
    var failures = 0
    func check(_ value: Bool, _ name: String) {
        print("\(value ? "ok  " : "FAIL") - \(name)")
        if !value { failures += 1 }
    }
    final class WaitingSource: MediaSource {
        let reading = DispatchSemaphore(value: 0)
        let stopped = DispatchSemaphore(value: 0)
        private let condition = NSCondition()
        private var ended = false
        private var stops = 0
        private var reads = 0
        var counts: (stops: Int, reads: Int) {
            condition.lock(); defer { condition.unlock() }
            return (stops, reads)
        }
        func read(_ maxBytes: Int) -> Data {
            condition.lock(); defer { condition.unlock() }
            reads += 1
            reading.signal()
            while !ended { condition.wait() }
            return Data()
        }
        func stop() {
            condition.lock()
            stops += 1
            ended = true
            condition.broadcast()
            condition.unlock()
            stopped.signal()
        }
    }
    func connectClient(_ port: UInt16, request: String) -> Int32? {
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { return nil }
        var timeout = timeval(tv_sec: 1, tv_usec: 0)
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        var address = sockaddr_in()
        address.sin_family = sa_family_t(AF_INET)
        address.sin_port = port.bigEndian
        inet_pton(AF_INET, "127.0.0.1", &address.sin_addr)
        let result = withUnsafePointer(to: &address) { p in
            p.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard result == 0 else { close(fd); return nil }
        let bytes = Array(request.utf8)
        let sent = bytes.withUnsafeBytes { Darwin.send(fd, $0.baseAddress, $0.count, 0) }
        guard sent == bytes.count else { close(fd); return nil }
        return fd
    }
    func connectionClosed(_ fd: Int32) -> Bool {
        var bytes = [UInt8](repeating: 0, count: 1024)
        while true {
            let count = recv(fd, &bytes, bytes.count, 0)
            if count == 0 { return true }
            if count < 0 { return errno == ECONNRESET }
        }
    }

    let source = WaitingSource()
    let server = StreamServer(makeSource: { source })
    guard let port = try? server.start(),
          let idleClient = connectClient(port, request: "GET /screen.ts HTTP/1.0\r\n"),
          let client = connectClient(port, request: "GET \(server.path) HTTP/1.0\r\n\r\n") else {
        print("FAIL - loopback fixture failed to connect"); server.stop(); return 1
    }
    check(source.reading.wait(timeout: .now() + 2) == .success, "client has an active source")
    server.stop()
    check(source.stopped.wait(timeout: .now() + 1) == .success, "Stop releases a source blocked in read")
    server.stop()
    check(source.counts.stops == 1, "repeated Stop disposes the source once")
    check(connectionClosed(client), "Stop closes an established streaming connection")
    check(connectionClosed(idleClient), "Stop releases clients with unfinished headers")
    source.stop() // also release the fixture when running against the unfixed implementation
    close(client)
    close(idleClient)

    let cancelled = StreamServer(makeSource: { WaitingSource() })
    cancelled.stop()
    check((try? cancelled.start()) == nil, "Stop before start cannot reopen a listener")
    cancelled.stop()

    let factoryEntered = DispatchSemaphore(value: 0)
    let factoryMayReturn = DispatchSemaphore(value: 0)
    let lateSource = WaitingSource()
    let late = StreamServer(makeSource: {
        factoryEntered.signal()
        _ = factoryMayReturn.wait(timeout: .now() + 5)
        return lateSource
    })
    if let latePort = try? late.start(),
       let lateClient = connectClient(latePort, request: "GET \(late.path) HTTP/1.0\r\n\r\n") {
        check(factoryEntered.wait(timeout: .now() + 2) == .success, "source factory is in progress")
        let stopReturned = DispatchSemaphore(value: 0)
        DispatchQueue.global().async { late.stop(); stopReturned.signal() }
        check(stopReturned.wait(timeout: .now() + 1) == .success, "Stop does not wait for the source factory")
        factoryMayReturn.signal()
        check(lateSource.stopped.wait(timeout: .now() + 2) == .success, "a source returned after Stop is disposed")
        check(lateSource.counts.reads == 0, "a source returned after Stop is never streamed")
        lateSource.stop()
        close(lateClient)
    } else { check(false, "late-source fixture connects"); factoryMayReturn.signal() }
    late.stop()

    final class CancelledLauncher: Launcher {
        var plays = 0
        func play(url: String, title: String) throws { plays += 1 }
        func stop() throws {}
        func nudge() throws {}
        func state() -> TransportState { .playing }
    }
    let launcher = CancelledLauncher()
    var serverStopped = false
    let session = Session(launcher: launcher, url: "http://127.0.0.1/screen.ts", log: { _ in },
                          shouldStop: { true }, onServerStop: { serverStopped = true })
    check(session.run() == 0 && launcher.plays == 0 && serverStopped,
          "a cancelled session stops its server without telling the TV to play")
    return failures
}

/// Exercise cancellation with a real anonymous pipe, without launching FFmpeg.
public func runCapturePipeSelfTests() -> Int {
    var failures = 0
    func check(_ value: Bool, _ name: String) {
        print("\(value ? "ok  " : "FAIL") - \(name)")
        if !value { failures += 1 }
    }
    let pipe = Pipe()
    let reader = CapturePipeReader()
    do {
        try reader.open(pipe.fileHandleForReading)
        try pipe.fileHandleForReading.close()
        try pipe.fileHandleForWriting.write(contentsOf: Data([1, 2, 3, 4]))
    } catch { print("FAIL - pipe fixture: \(error)"); return 1 }
    defer { reader.close(); try? pipe.fileHandleForWriting.close() }
    check(reader.read(2) == Data([1, 2]), "capture read respects its byte limit")
    check(reader.read(2) == Data([3, 4]), "capture reader owns its descriptor")

    let entered = DispatchSemaphore(value: 0)
    let returned = DispatchSemaphore(value: 0)
    let empty = AtomicFlag()
    DispatchQueue.global().async {
        entered.signal()
        empty.set(reader.read(188).isEmpty)
        returned.signal()
    }
    check(entered.wait(timeout: .now() + 1) == .success, "pipe reader starts")
    check(returned.wait(timeout: .now() + 0.1) == .timedOut, "pipe read waits for bytes")
    reader.close()
    // Keep the writer open: cancellation must not depend on FFmpeg exiting.
    check(returned.wait(timeout: .now() + 1) == .success && empty.get(),
          "closing capture output cancels a blocked read")
    reader.close()
    check(reader.read(188).isEmpty, "reads after close return EOF")
    do {
        let other = Pipe()
        try reader.open(other.fileHandleForReading)
        check(false, "closed capture output cannot reopen")
    } catch { check(true, "closed capture output cannot reopen") }
    return failures
}
