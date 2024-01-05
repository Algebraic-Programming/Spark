
package com.huawei.graphblas.examples.cmdargs

import com.huawei.graphblas.examples.cmdargs.PageRankInput
import scopt.{ OParser, OParserSetup, DefaultOParserSetup, OptionDef }

import java.lang.IllegalArgumentException

trait PageRankParser[ ArgsT <: PageRankInput ] {

	def makeParser[ T <: ArgsT ](): List[OptionDef[_, T]]

	def makeDefaultObject(): ArgsT

	def parseArguments( args: Array[String] ): ArgsT = {
		val progBuilder = OParser.builder[ ArgsT ]
		val progParser: OParser[ Unit, ArgsT] = {
			import progBuilder._
			OParser.sequence(
				programName("<program>")
			)
		}

		val argBuilder = OParser.builder[ ArgsT ]
		val argParser = {
			import argBuilder._
			OParser.sequence(
				help('h', "help")
					.text("print this help"),
				arg[String]("<input file>...").required()
					.minOccurs(1)
					.action((x, c) => {c.inputFiles = c.inputFiles :+ x; c})
					.text( "optional unbounded args" )
			)
		}

		val parser = OParser( progParser.toList( 0 ), makeParser[ ArgsT ]() ++ argParser.toList )
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
