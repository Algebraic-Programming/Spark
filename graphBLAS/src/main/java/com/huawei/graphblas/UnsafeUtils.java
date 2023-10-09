package com.huawei.graphblas;

import sun.misc.Unsafe;
import java.lang.reflect.Field;

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

}
