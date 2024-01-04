
package com.huawei.graphblas.examples.cmdargs

// import scopt.OParser
import scopt.{ OParser, OParserSetup, DefaultOParserSetup }

import java.lang.IllegalArgumentException

trait PageRankParser[ ArgsT ] {

	def makeParser[ T <: ArgsT ](): OParser[ Unit, T ]

	// def buildParser(): OParser[ Unit, ArgsT ] = makeParser[ ArgsT ]()

	def makeDefaultObject(): ArgsT

	def parseArguments( args: Array[String] ): ArgsT = {

		val parser = makeParser[ ArgsT ]()

		val setup: OParserSetup = new DefaultOParserSetup {
			override def showUsageOnError = Some(true)
		}

		val parsedOpt: Option[ ArgsT ] = OParser.parse[ ArgsT ](parser, args, makeDefaultObject(), setup )
			
		if( ! parsedOpt.isDefined ) {
			System.exit(1)
		}
		parsedOpt.get
	}
}
