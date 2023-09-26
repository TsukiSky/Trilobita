package com.trilobita.commons;

import java.util.List;

public interface Combinable {
    Message<?> combine(Message<?> messages);
}
