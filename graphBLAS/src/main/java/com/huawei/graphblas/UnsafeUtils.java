package com.huawei.graphblas;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.util.Arrays;

class UnsafeUtils {

	public static Unsafe getTheUnsafe() {
		sun.misc.Unsafe unsafe;
		try {
			Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
			unsafeField.setAccessible(true);
			unsafe = (sun.misc.Unsafe) unsafeField.get(null);
		} catch (Throwable cause) {
			unsafe = null;
		}
		return unsafe;
	}

	private static long DOUBLE_SIZE = 8;
	private static long INT_SIZE = 4;

	public static double[] makeDoubleArray( long initAddr, long size ) {
		double[] res = new double[(int)size];

		Unsafe un = getTheUnsafe();
		for(int i = 0; i < size; i++) {
			res[ i ] = un.getDouble( initAddr );
			initAddr += DOUBLE_SIZE;
		}
		return res;
	}

	public static int[] makeIntArray( long initAddr, long size ) {
		int[] res = new int[ (int)size ];
		Arrays.fill( res, 0 );

		Unsafe un = getTheUnsafe();
		for(int i = 0; i < size; i++) {
			res[ i ] = un.getInt( initAddr );
			initAddr += INT_SIZE;
		}
		return res;
	}
}
