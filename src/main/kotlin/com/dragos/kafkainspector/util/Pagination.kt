package com.dragos.kafkainspector.util

data class PageRequest(val page: Int = 0, val size: Int = 50) {
    val offset: Int get() = page * size
}

data class PageResponse<T>(val items: List<T>, val page: Int, val size: Int, val total: Int)
