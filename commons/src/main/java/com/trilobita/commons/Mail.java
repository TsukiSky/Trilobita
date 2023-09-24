package com.trilobita.commons;

import lombok.Data;

@Data
public class Mail {
    Integer from;
    Integer to;
    Message content;
}
