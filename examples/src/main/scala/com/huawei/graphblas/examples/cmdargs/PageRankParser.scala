
package com.huawei.graphblas.examples.cmdargs

import com.huawei.graphblas.examples.cmdargs.PageRankInput
import scopt.{ OParser, OParserSetup, DefaultOParserSetup, OptionDef }

import java.lang.IllegalArgumentException

trait PageRankParser[ ArgsT <: PageRankInput ] {

	def makeParser[ T <: ArgsT ](): List[OptionDef[_, T]]

	def makeDefaultObject(): ArgsT

	def parseArguments( args: Array[String], maxArgs: Option[ Int ] = Option.empty[ Int ] ): ArgsT = {

		val minArgs = 1

		if( maxArgs.isDefined && maxArgs.get < minArgs ) {
			throw new IllegalArgumentException( s"max number of arguments (${maxArgs}) is smaller than the minimum number of arguments (${minArgs})" )
		}

		val progBuilder = OParser.builder[ ArgsT ]
		val progParser: OParser[ Unit, ArgsT] = {
			import progBuilder._
			OParser.sequence(
				programName("<program>")
			)
		}

		val maxNumArgas = maxArgs.getOrElse( Int.MaxValue )

		val argBuilder = OParser.builder[ ArgsT ]
		val argParser = {
			import argBuilder._
			OParser.sequence(
				help('h', "help")
					.text("print this help"),
				arg[String]( if ( maxNumArgas > 1 ) "<input file(s)>..." else "<input file>" ).required()
					.minOccurs(1)
					.maxOccurs( maxNumArgas )
					.action((x, c) => {c.inputFiles = c.inputFiles :+ x; c})
					.text( if ( maxNumArgas > 1 ) "one or more input files, space-separated" else "input file" )
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
