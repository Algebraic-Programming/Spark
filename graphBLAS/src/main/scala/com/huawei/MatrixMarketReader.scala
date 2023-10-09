package com.huawei

import java.lang.Exception

import scala.io.Source
import scala.util.Using

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph


import com.huawei.RDDSparseMatrix

object MatrixMarketReader {
	/**
	 * Given a 3-tuple in string form, translate it into a coordinate-value pair.
	 *
	 * @param[in] x A string consisting of a single line, two positive non-zero
	 *              integers separated and followed by a space, and one floating
	 *              point value closing off the string.
	 *
	 * @returns A 3-tuple (i,j,v), where (i,j) are the two integers (in the same
	 *          order) and v is the floating point value in double precision.
	 */
	private def parseTriple( x: String ) : (Long, Long, Double) = {
		x.split(" ") match {
			case Array( a, b, c ) => (a.toLong - 1, b.toLong - 1, c.toDouble)
			case Array( a, b ) => (a.toLong - 1, b.toLong - 1, 1.0)
			case _ => throw new Exception( "Cannot parse matrix market file at line " + x )
		}
	}

	/**
	 * Given a 3-tuple in string form, translate it into one or two coordinate-
	 * value pairs. One pair is returned if and only if its coordinates are
	 * equal; otherwise both the original coordinate-value pair is joined by its
	 * transposed nonzero.
	 *
	 * @param[in] x A string consisting of a single line, two positive non-zero
	 *              integers separated and followed by a space, and one floating
	 *              point value closing off the string.
	 *
	 * @returns If the two integers are equal: a 3-tuple (i,i,v), where (i,i) are
	 *          the two integers and v is the floating point value in double
	 *          precision. The tuple is packed in an array of size one.
	 * @returns Otherwise: an array with two 3-tuples (i,j,v) and (j,i,v).
	 */
	private def parseSymTriple( x: String ) : Array[(Long, Long, Double)] = {
		val orig = parseTriple(x)
		if( orig._1 == orig._2 ) {
			Array( orig )
		} else {
			Array( orig, (orig._2, orig._1, orig._3) )
		}
	}

	/**
	 * Parses the header of a MatrixMarket file.
	 *
	 * @param[in] in The MatrixMarket file as an RDD of strings.
	 *
	 * @returns A 3-tuple \f$ (m,n,nnz) \f$.
	 */
	private def parseHeader( in: RDD[String] ): (Long, Long, Long) = {
		val subRDD = in.mapPartitions( iterator => {
			val first = iterator.take(1)
			if( first.hasNext ) {
				if( first.next().startsWith( "%" ) ) {
					val filtered = iterator.dropWhile( x => x.startsWith( "%" ) )
					assert( filtered.hasNext ) //this could in principle fail if you are very very very unlucky. In that case, change the number of parts (decrease by 1, e.g.) and you should be fine.
					val sz_header = filtered.next().split( " " )
					println( sz_header.flatten )
					assert( sz_header.size == 3 )
					Iterator((sz_header(0).toLong, sz_header(1).toLong, sz_header(2).toLong))
				} else {
					Iterator()
				}
			} else {
				Iterator()
			}
		} );
		subRDD.max()
	}

	/**
	 * Strips the header from a MatrixMarket file.
	 *
	 * @param[in] in The MatrixMarket file as an RDD of strings.
	 *
	 * @returns An RDD of strings with the contents of the MatrixMarket file,
	 *          without its header line.
	 */
	private def filterHeader( in: RDD[String] ): RDD[String] = {
		in.mapPartitions( iterator => {
			if( iterator.hasNext ) {
				var line = iterator.next()
				if( line.startsWith( "%" ) ) {
					val filtered = iterator.dropWhile( x => x.startsWith( "%" ) )
					assert( filtered.hasNext ) //see above comment
					filtered.next()
					filtered
				} else {
					Iterator( line ) ++ iterator
				}
			} else {
				Iterator()
			}
		} )
	}

	/**
	 * Turns an array of 3-tuples where the first element of each tuple is equal
	 * into an array of 2-tuples.
	 *
	 * @param[in] rowID The row index. Each first element of each given tuple is
	 *                  equal to this value.
	 * @param[in] x     An array of 3-tuples of which each first entry should
	 *                  equal \a rowID.
	 *
	 * @returns A 3-tuple where the first entry equals \a rowID while the second
	 *          entry is an array of column indices (the second entry of each
	 *          input tuple) and the third entry is an array of values (the third
	 *          entry of each input tuple). The order of array entries is
	 *          retained.
	 *
	 * For example: [(1,2,3.5),(1,5,0.1),(1,3,1.1)] is turned into
	 *              (1,[2 5 3], [3.5 0.1 1.1])
	 */
	private def packTriples( rowID: Long, x: Iterable[(Long, Long, Double)] ) : (Long, Iterable[Long], Iterable[Double]) = {
		val unzipped : (Iterable[Long], Iterable[Double]) = x.map( x => (x._2, x._3) ).unzip
		(rowID, unzipped._1, unzipped._2)
	}

	/**
	 * Reads a non-symmetric MatrixMarket file.
	 *
	 * @param[in] sc The Spark context.
	 * @param[in] fn The path to the MatrixMarket file.
	 *
	 * @returns A 4-tuple. The first entry contains the number of matrix rows,
	 *          the second the number of matrix columns, the third the number of
	 *          nonzeroes. The fourth entry is an RDD of 3-tuples formatted as per
	 *          #packTriples.
	 */
	private def readCoordMatrix( sc: SparkContext, fn: String ) : RDDSparseMatrix = {
		val fnRDD = sc.textFile( fn )
		val header = parseHeader( fnRDD )
		val parsed = filterHeader( fnRDD )
		val ret = parsed.filter( x => !x.startsWith("%") ).map( parseTriple ).groupBy( x => x._1 ).map( x => packTriples(x._1, x._2) )
		new RDDSparseMatrix(header._1, header._2, header._3, ret)
	}

	/**
	 * Reads a symmetric MatrixMarket file.
	 *
	 * @see #readCoordMatrix.
	 */
	private def readSymCoordMatrix( sc: SparkContext, fn: String ) : RDDSparseMatrix = {
		val fnRDD = sc.textFile( fn, sc.defaultParallelism );
		val header = parseHeader( fnRDD );
		val parsed = filterHeader( fnRDD );
		val ret = parsed.filter( x => !x.startsWith("%") ).flatMap( parseSymTriple ).groupBy( x => x._1 ).map( x => packTriples(x._1, x._2) )
		new RDDSparseMatrix(header._1, header._2, header._3, ret)
	}

	/**
	 * Reads a non-symmetric MatrixMarket file and interprets it as a pattern
	 * matrix, thus ignoring any nonzero values that may be in the file.
	 *
	 * @param[in] sc The Spark context.
	 * @param[in] fn The path to the MatrixMarket file.
	 *
	 * @returns Nonzeroes are grouped by matrix row-- the first entry is a row
	 *          index, while the second entry is an array of column indices with
	 *          nonzeroes on that row.
	 */
	private def readGenPatternMatrix( sc: SparkContext, fn: String, P: Int = 0 ) : RDD[ (Long, Iterable[Long], Iterable[Double] ) ] = {
		val file = if(P == 0) sc.textFile(fn, sc.defaultParallelism) else sc.textFile( fn, P )
		val parsed = filterHeader( file )
		parsed.filter( x => !x.startsWith( "%" ) ).map( parseTriple ).groupBy( x => x._1 ).map( x => packTriples( x._1, x._2 ) )
	}

	private def readSymPatternMatrix( sc: SparkContext, fn: String, P: Int = 0 ) : RDD[ (Long, Iterable[Long], Iterable[Double] ) ] = {
		val file = if(P == 0) sc.textFile( fn, sc.defaultParallelism ) else sc.textFile( fn, P )
		val parsed = filterHeader( file )
		parsed.filter( x => !x.startsWith("%") ).flatMap( parseSymTriple ).groupBy( x => x._1 ).map( x => packTriples( x._1, x._2 ) )
	}

	private def readPatternMatrix( sc : SparkContext, fn: String, symmetric: Boolean ): RDDSparseMatrix = {
		val fnRDD = sc.textFile( fn, sc.defaultParallelism )
		val header = parseHeader( fnRDD )
		val retRDD: RDD[ (Long, Iterable[Long], Iterable[Double]) ] =
		if( symmetric ) {
			readSymPatternMatrix( sc, fn )
		} else {
			readGenPatternMatrix( sc, fn )
		}
		new RDDSparseMatrix( header._1, header._2, header._3, retRDD )
	}

	def readMM( sc: SparkContext, fn: String ) : RDDSparseMatrix = {
		Using.resource( Source.fromFile( fn ) ) { file => {
			val line = file.getLines().next()
			if( !line.contains( "MatrixMarket" ) )
				throw new Exception( "This parser only understands some MatrixMarket formats" )
			if( !line.contains( "matrix" ) )
				throw new Exception( "Expected a MatrixMarket matrix object" )
			if( !line.contains( "coordinate" ) )
				throw new Exception( "This parser only understands coordinate MatrixMarket formats" )
			if( line.contains( "pattern" ) ) {
				if( line.contains( "symmetric" ) ) {
					println( "Symmetric pattern MatrixMarket matrix detected; parsing..." )
					return readPatternMatrix( sc, fn, true )
				} else {
					if( !line.contains( "general" ) )
						throw new Exception( "This parser only supports symmetric or general (pattern) MatrixMarket formats" )
					return readPatternMatrix( sc, fn, false )
				}
			}
			if( line.contains( "symmetric" ) ) {
				println( "Symmetric MatrixMarket matrix detected; parsing..." )
				return readSymCoordMatrix( sc, fn )
			} else {
				if( !line.contains( "general" ) )
					throw new Exception( "This parser only supports symmetric or general MatrixMarket formats" )
				return readCoordMatrix( sc, fn )
			}
		} }
	}

	private def matrix2GraphXEdge( in: RDD[ (Long, Iterable[Long], Iterable[Double]) ] ) : RDD[ Edge[Double] ] = {
		in.flatMap( row => {
			( row._2 zip row._3 ).map( rowValue => Edge( row._1, rowValue._1, rowValue._2 ) )
		} )
	}

	private def matrix2GraphXVertex( sc: SparkContext, in: RDDSparseMatrix ) :
		RDD[(Long,Double)] = {
		if( in.m != in.n )
			throw new Exception( "Input matrix is not square!" )
		sc.range( 1, in.m + 1 ).map( x => ( x, 0.0 ) )
	}

	def matrix2GraphX( sc: SparkContext, in: RDDSparseMatrix ) : Graph[Double,Double] = {
		val edges = matrix2GraphXEdge( in.data )
		println( "Loading " + edges.count() + " nonzeroes into GraphX" )
		Graph(
			matrix2GraphXVertex( sc, in ),
			edges
		)
	}

}
