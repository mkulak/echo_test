package core

import co.paralleluniverse.fibers.Suspendable

// Unable to avoid this pervasive @Suspendable due to Quasar special requirements

interface IActorData {
    val shouldRun:Boolean
}

interface IActor<Data : IActorData> {
    @Suspendable fun init():Data
    @Suspendable fun tick(data: Data): Data
}

interface IActorSystem {
    @Suspendable fun <Data : IActorData> spawn(id:Int, actor:IActor<Data>)
    @Suspendable fun receive(id:Int, timeout:Long):Any?
    @Suspendable fun send(to:Int, msg:Message)
    @Suspendable fun now():Long
    @Suspendable fun sleep(millis:Long)
    @Suspendable fun log(id: Int?, text: String)
}

