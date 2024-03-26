//package ru.quipy.payments.config
//
//import org.springframework.scheduling.annotation.Async
//import org.springframework.stereotype.Service
//import ru.quipy.payments.logic.ExternalServiceProperties
//import java.util.concurrent.atomic.AtomicLong
//
//@Service
//class AccountService {
//    val accounts = ExternalServicesConfig.accounts.map { AccountWrapper(it) }
//
//    @Async
//    fun getAvailableAccount(): ExternalServiceProperties? {
//        return accounts.filter { it.isAccountAvailable() }
//            .minByOrNull { it.account.callCost }?.apply { this.registerRequest() }?.account
//    }
//}
//
//class AccountWrapper(
//    val account: ExternalServiceProperties
//) {
//    private val lastRequestTime = AtomicLong(System.currentTimeMillis())
//    private val requestsInLastSecondCounter = AtomicLong(0)
//    fun isAccountAvailable(): Boolean {
//        val currentTime = System.currentTimeMillis()
//        if (currentTime - lastRequestTime.get() > 1000L) {
//            lastRequestTime.set(currentTime)
//            requestsInLastSecondCounter.set(0)
//        }
//        return requestsInLastSecondCounter.get() < account.rateLimitPerSec
//    }
//
//    fun registerRequest() {
//        requestsInLastSecondCounter.incrementAndGet()
//    }
//}
