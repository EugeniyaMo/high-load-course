package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val state: AccountState,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>



    fun selectAvailableAccount(
        accountState: AccountState,
        paymentStartedAt: Long,
        paymentId: UUID,
        transactionId: UUID
    ): AccountState? {

        if (accountState.properties.request95thPercentileProcessingTime + Duration.ofMillis(now() - paymentStartedAt)
            > paymentOperationTimeout) {
            return processNext(accountState, paymentStartedAt, paymentId, transactionId,
                "Request timeout")
        }

        if (accountState.currentRequestCount.get() > accountState.properties.theoreticalSpeed) {
            return processNext(accountState, paymentStartedAt, paymentId, transactionId,
                "Current request count exceeds theoretical speed")
        }

        if (!accountState.rateLimiter.tick()) {
            return processNext(accountState, paymentStartedAt, paymentId, transactionId,
                "Rate limit exceeded")
        }

        if (!accountState.window.tryAcquire()) {
            return processNext(accountState, paymentStartedAt, paymentId, transactionId,
                "Parallel requests exceed")
        }

        return accountState
    }

    private fun processNext(
        accountState: AccountState,
        paymentStartedAt: Long,
        paymentId: UUID,
        transactionId: UUID,
        message: String
    ): AccountState? {
        if (accountState.getNext() != null) {
            return selectAvailableAccount(accountState.getNext()!!, paymentStartedAt, paymentId, transactionId)
        } else {

            Thread.sleep((1 / accountState.properties.rateLimitPerSec).toLong() * 1000)
            return selectAvailableAccount(accountState, paymentStartedAt, paymentId, transactionId)
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {

        val transactionId = UUID.randomUUID()
        val accountForRequest =
            selectAvailableAccount(state, paymentStartedAt, paymentId, transactionId) ?: return

        val accountName = accountForRequest.properties.accountName
        val serviceName = accountForRequest.properties.serviceName

        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        accountForRequest.currentRequestCount.getAndIncrement()

        val httpClientExecutor = Executors.newFixedThreadPool(accountForRequest.properties.parallelRequests,
            NamedThreadFactory("http"))

        val client = OkHttpClient.Builder().writeTimeout(80, TimeUnit.SECONDS).run {
            dispatcher(Dispatcher(httpClientExecutor))
            build()
        }

        client.newCall(request).enqueue(
            object: Callback {
                override fun onFailure(call: Call, e: IOException) {
                    accountForRequest.currentRequestCount.getAndDecrement()
                    onFailure(call, e, transactionId, paymentId, accountForRequest)
                }

                override fun onResponse(call: Call, response: Response) {
                    accountForRequest.currentRequestCount.getAndDecrement()
                    onResponse(call, response, accountForRequest, transactionId, paymentId)
                }
            }
        )
    }

    fun onFailure(call: Call, e: IOException, transactionId: UUID, paymentId: UUID, accountState: AccountState) {
        accountState.window.release()
        logger.error("Request timeout!")
        when (e) {
            is SocketTimeoutException -> {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
            }

            else -> {
                logger.error(
                    "[${accountState.properties.accountName}] Payment failed for txId: $transactionId, payment: $paymentId",
                    e
                )
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
        }
    }

    fun onResponse(call: Call, response: Response, accountState: AccountState, transactionId: UUID, paymentId: UUID) {
        accountState.window.release()
        val body = try {
            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[${accountState.properties.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
            ExternalSysResponse(false, e.message)
        }

        logger.warn("[$accountState.properties.accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }
    }
}

public fun now() = System.currentTimeMillis()