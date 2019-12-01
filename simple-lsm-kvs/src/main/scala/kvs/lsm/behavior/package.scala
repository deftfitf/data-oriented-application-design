package kvs.lsm

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{Behavior, SupervisorStrategy}

import scala.reflect.ClassTag

package object behavior {

  implicit class SuperviseBehavior[T](behavior: Behavior[T]) {

    def superviseOnFailure[Thr <: Throwable: ClassTag](
        strategy: SupervisorStrategy): Behavior[T] =
      Behaviors.supervise(behavior).onFailure[Thr](strategy)

  }

}
