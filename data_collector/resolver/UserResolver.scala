package plista.ml.resolver

import com.plista.thrift.converter.SlimEventContext

object UserResolver {
  def apply(slimEventContext: SlimEventContext): Seq[Long] = {
    val idFromContext: Int => Long = slimEventContext.environment.ids.getOrElse(_ :Int, 0)

    val browser = idFromContext(4)
    val isp = idFromContext(5)
    val os = idFromContext(6)
    val geoUser = idFromContext(7)
    val geoUserCountry = idFromContext(15)
    val langUser = idFromContext(17)
    val deviceType = idFromContext(47)
    val device = idFromContext(82)

    val geoUserZip: Long = slimEventContext.environment.integers.getOrElse(22, 0)
    val userSubscriptionStatus: Long = slimEventContext.environment.integers.getOrElse(141, 0)

    Seq(
      browser,
      isp,
      os,
      geoUser,
      geoUserCountry,
      langUser,
      deviceType,
      device,
      geoUserZip,
      userSubscriptionStatus
    )
  }
}
