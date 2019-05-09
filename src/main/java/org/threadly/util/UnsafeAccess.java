package org.threadly.util;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import sun.misc.Unsafe;

/**
 * Used for internal access to UNSAFE actions.  This uses the {@link sun.misc.Unsafe} class to do 
 * visibility modifications and potentially other low level access.
 * 
 * @since 5.28
 */
public final class UnsafeAccess {
  private static final Unsafe UNSAFE;
  private static final Method SET_ACCESSIBLE;
  
  static {
    try {
      Field theUnsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafeField.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafeField.get(null);
      
      /*Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
      unsafeConstructor.setAccessible(true);
      UNSAFE = unsafeConstructor.newInstance();*/
    } catch (NoSuchFieldException | SecurityException | 
             IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException("Unsupported JVM version, please update threadly or file an issue" + 
                                   "...Could not get Unsafe reference", e);
    }
    
    Method setAccessible;
    try {
      // setAccessible0 will avoid SecurityManager permission check, avoiding jdk9+ errors
      setAccessible = AccessibleObject.class.getDeclaredMethod("setAccessible0", boolean.class);
      
      Field methodModifiers = Method.class.getDeclaredField("modifiers");
      long methodModifiersOffset = UNSAFE.objectFieldOffset(methodModifiers);
      
      UNSAFE.getAndSetInt(setAccessible, methodModifiersOffset, Modifier.PUBLIC);
    } catch (NoSuchMethodException | NoSuchFieldException | SecurityException e) {
      try {
        // makes usable for eclipse jvm
        setAccessible = AccessibleObject.class.getDeclaredMethod("setAccessible", boolean.class);
      } catch (NoSuchMethodException | SecurityException e1) {
        e1.initCause(e);
        throw new RuntimeException("Unsupported JVM version, please update threadly or file an issue" + 
                                      "...Could not set setAccessible to public", e1);
      }
    }
    SET_ACCESSIBLE = setAccessible;
  }
  
  /**
   * Takes in a {@link Field} and sets the accessibility to be public.
   * 
   * @param f Field to be modified
   */
  public static void setFieldToPublic(Field f) {
    try {
      SET_ACCESSIBLE.invoke(f, true);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException("Unsupported JVM version, please update threadly or file an issue" + 
                                    "...Could not set Field to public: " + f, e);
    }
  }
}
