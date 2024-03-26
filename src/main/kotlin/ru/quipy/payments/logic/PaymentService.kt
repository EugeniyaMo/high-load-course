package ru.quipy.payments.logic

import java.time.Duration
import java.util.*
import kotlin.math.min

interface PaymentService {
    /**
     * Submit payment request to external service.
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long)
}

interface PaymentExternalService : PaymentService

/**
 * Describes properties of payment-provider accounts.
 */
data class ExternalServiceProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val request95thPercentileProcessingTime: Duration = Duration.ofSeconds(11),
    val cost: Int,
    val next: ExternalServiceProperties?,
    val theoreticalSpeed: Long = min(rateLimitPerSec.toLong(),
        parallelRequests * Duration.ofSeconds(1).toMillis() / request95thPercentileProcessingTime.toMillis())
)

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val result: Boolean,
    val message: String? = null,
)