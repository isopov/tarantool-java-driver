package com.sopovs.moradanen.tarantool.core;

import java.lang.annotation.*;

import javax.annotation.Nonnull;
import javax.annotation.meta.TypeQualifierDefault;

@Target(ElementType.PACKAGE)
@Documented
@Nonnull
@TypeQualifierDefault({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface NonNullApi {
}
