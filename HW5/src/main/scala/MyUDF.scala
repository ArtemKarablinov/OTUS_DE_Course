import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object MyUDF {

    def ipToInt(ipString: String): Long = {
        val ipGroups = ipString
            .split("\\.")
            .map(x => x.toLong)

    assert(ipGroups.length == 4)

    (ipGroups(0) << 24) + (ipGroups(1) << 16) + (ipGroups(2) << 8) + ipGroups(3)
    }

    def intToIp(int: Long): String = {
        val bin = int.toBinaryString
        val leadingZeros = "0" * (32 - bin.size)

    (leadingZeros + bin)
        .split("(?<=\\G.{8})")
        .map(Integer.parseInt(_, 2))
        .mkString(sep=".")
    }

    val ipToIntUDF: UserDefinedFunction = udf(ipToInt _)
    val intToIpUDF: UserDefinedFunction = udf(intToIp _)

}
