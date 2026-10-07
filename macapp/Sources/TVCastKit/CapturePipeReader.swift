import Foundation

/// A capture-output pipe whose blocked reader can be cancelled from another thread.
/// Each read owns a duplicate fd, preventing close/reuse races during Stop.
final class CapturePipeReader {
    private let lock = NSLock()
    private var fd: Int32 = -1
    private var closed = false

    func open(_ handle: FileHandle) throws {
        let copy = dup(handle.fileDescriptor)
        guard copy >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EBADF) }
        lock.lock(); defer { lock.unlock() }
        guard !closed, fd < 0 else { Darwin.close(copy); throw POSIXError(.EBADF) }
        fd = copy
    }

    func read(_ maxBytes: Int) -> Data {
        guard maxBytes > 0 else { return Data() }
        let reader = lock.withLock { closed || fd < 0 ? -1 : dup(fd) }
        guard reader >= 0 else { return Data() }
        defer { Darwin.close(reader) }
        var pollFD = pollfd(fd: reader, events: Int16(POLLIN), revents: 0)
        while !lock.withLock({ closed }) {
            let ready = poll(&pollFD, 1, 100)
            if ready < 0 {
                if errno == EINTR { continue }
                return Data()
            }
            if ready == 0 { continue }
            if lock.withLock({ closed }) { return Data() }
            var bytes = [UInt8](repeating: 0, count: maxBytes)
            let count = Darwin.read(reader, &bytes, bytes.count)
            if count < 0 && errno == EINTR { continue }
            return count > 0 ? Data(bytes.prefix(count)) : Data()
        }
        return Data()
    }

    func close() {
        let old = lock.withLock {
            closed = true
            let old = fd
            fd = -1
            return old
        }
        if old >= 0 { Darwin.close(old) }
    }

    deinit { close() }
}
