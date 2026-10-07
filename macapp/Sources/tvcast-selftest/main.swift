import TVCastKit
import Foundation
exit((runSelfTests() + runServerSelfTest() + runSessionSelfTest() + runCaptureSelfTest()
      + runLifecycleSelfTests() + runCapturePipeSelfTests()) == 0 ? 0 : 1)
