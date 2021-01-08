package com.thoughtworks.exception;

/**
 * @author: spring du
 * @description: 异常基类
 * @date: 2021/1/6 16:54
 */
public class BaseException extends RuntimeException {

    public BaseException() {
    }

    public BaseException(String message) {
        super(message);
    }

    public BaseException(Throwable cause) {
        super(cause);
    }

    public BaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
