package com.twitter.zipkin.dependencies

import java.util.Date

import com.twitter.algebird.{Semigroup, Monoid}
import com.twitter.scalding._
import com.twitter.zipkin.common.{Dependencies, DependencyLink, Span}

final class ZipkinDependenciesJob
   (args: Args) extends Job(args)
{

  implicit val sg: Semigroup[DependencyLink] = new Semigroup[DependencyLink] {
    override def plus(l: DependencyLink, r: DependencyLink) =
      DependencyLink(l.parent, l.child, l.callCount + r.callCount)
  }

  implicit val monoid: Monoid[Dependencies] = new Monoid[Dependencies] {
    override def zero = Dependencies.zero

    override def plus(l: Dependencies, r: Dependencies) = l + r
  }

  val dateRange: DateRange = DateRange(new Date(0L), new Date)

  @transient
  val (extraConfig, spanSource) = SpanSourceProvider(args)

  override def config = super.config ++ extraConfig

  val allSpans = TypedPipe.from(spanSource)
    .groupBy { span: Span => (span.id, span.traceId) }
    .reduce { (s1, s2) => s1.mergeSpan(s2) }
    .filter { case (key, span) => span.isValid }

  val parentSpans = allSpans

  val childSpans = allSpans
    .filter { case (key, span) => span.parentId.isDefined }
    .mapValues { span => ((span.parentId.get, span.traceId), span)}

  val result = parentSpans.join(childSpans)
    .mapValues { case (_, (parent: Span, child: Span)) =>
    // We consider non-zero durations calls.
    val callCount = child.duration.map(_ => 1).getOrElse(0)
    val dlink = DependencyLink(parent.serviceName.get, child.serviceName.get, callCount)
    ((parent.serviceName.get, child.serviceName.get), dlink)
  }
    .sum
    .values
    .map { dlink => Dependencies(dateRange.start.timestamp * 1000, dateRange.end.timestamp * 1000, Seq(dlink._2)) }
    .sum

  result.write(spanSource)
}

object SpanSourceProvider {
  def apply(args: Args) : (Map[AnyRef,AnyRef], Source with TypedSource[Span] with TypedSink[Dependencies]) = args.required("source") match {
// @fixme needs to be rewritten to use CqlInputFormat
//    case "cassandra" => {
//      (Map("hosts" -> args.required("hosts"), "port" -> args.getOrElse("port", "9160")), new cassandra.SpanSource)
//    }
    case s:String => throw new ArgsException(s+" is not an implemented source.")
  }
}