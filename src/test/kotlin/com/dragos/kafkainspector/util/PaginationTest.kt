package com.dragos.kafkainspector.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PaginationTest {
    @Test
    fun `offset is derived from page and size`() {
        val request = PageRequest(page = 2, size = 25)

        assertEquals(50, request.offset)
    }

    @Test
    fun `page response exposes supplied metadata`() {
        val response = PageResponse(items = listOf("a", "b"), page = 1, size = 10, total = 15)

        assertEquals(listOf("a", "b"), response.items)
        assertEquals(1, response.page)
        assertEquals(10, response.size)
        assertEquals(15, response.total)
    }
}
