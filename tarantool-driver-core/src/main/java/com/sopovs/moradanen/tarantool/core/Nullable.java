package com.sopovs.moradanen.tarantool.core;

import javax.annotation.Nonnull;
import javax.annotation.meta.TypeQualifierNickname;
import javax.annotation.meta.When;
import java.lang.annotation.*;

@Target({ElementType.METHOD, ElementType.PARAMETER, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Nonnull(when = When.MAYBE)
@TypeQualifierNickname
public @interface Nullable {
}