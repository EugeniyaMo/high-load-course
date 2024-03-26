package ru.quipy.payments.logic

import ru.quipy.common.utils.CountingRateLimiter
import ru.quipy.common.utils.OngoingWindow
import java.util.concurrent.atomic.AtomicInteger

class AccountState(
    val properties: ExternalServiceProperties
) {

    val window = OngoingWindow(properties.parallelRequests)
    val rateLimiter = CountingRateLimiter(properties.rateLimitPerSec)
    val currentRequestCount = AtomicInteger(0)

    fun getNext(): AccountState? {
        if (properties.next == null) {
            return null
        }

        return AccountState(properties.next)
    }
}