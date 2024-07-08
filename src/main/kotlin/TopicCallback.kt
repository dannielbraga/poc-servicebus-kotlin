package gb.tech

import com.azure.messaging.servicebus.*
import java.util.concurrent.TimeUnit

fun main() {

    val connectionString = "Endpoint=sb://veripag-split-br-so.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=oDUiyFd6ncFj72HneCcdPhve10hVX6dsYzsgayR6D1s="
    val topicName = "sb-topic-sale-callback"
    val subName = "sb-subscription-sale-callback"

    val processorClient = ServiceBusClientBuilder()
        .connectionString(connectionString)
        .processor()
        .topicName(topicName)
        .subscriptionName(subName)
        .processMessage { context: ServiceBusReceivedMessageContext? ->
            if (context != null) {
                processMessage(
                    context
                )
            }
        }
        .processError { context: ServiceBusErrorContext? ->
            if (context != null) {
                processError(
                    context
                )
            }
        }
        .buildProcessorClient()

    println("Starting the processor")
    processorClient.start()

    TimeUnit.SECONDS.sleep(10)
    println("Stopping and closing the processor")
    processorClient.close()
}

private fun processMessage(context: ServiceBusReceivedMessageContext) {
    val message = context.message
    System.out.printf(
        "Processing message. Session: %s, Sequence #: %s. Contents: %s%n", message.messageId,
        message.sequenceNumber, message.body
    )
}

private fun processError(context: ServiceBusErrorContext) {
    System.out.printf(
        "Error when receiving messages from namespace: '%s'. Entity: '%s'%n",
        context.fullyQualifiedNamespace, context.entityPath
    )

    if (context.exception !is ServiceBusException) {
        System.out.printf("Non-ServiceBusException occurred: %s%n", context.exception)
        return
    }

    val exception = context.exception as ServiceBusException
    val reason = exception.reason

    if (reason === ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED || reason === ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND || reason === ServiceBusFailureReason.UNAUTHORIZED) {
        System.out.printf(
            "An unrecoverable error occurred. Stopping processing with reason %s: %s%n",
            reason, exception.message
        )
    } else if (reason === ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
        System.out.printf("Message lock lost for message: %s%n", context.exception)
    } else if (reason === ServiceBusFailureReason.SERVICE_BUSY) {
        try {
            // Choosing an arbitrary amount of time to wait until trying again.
            TimeUnit.SECONDS.sleep(1)
        } catch (e: InterruptedException) {
            System.err.println("Unable to sleep for period of time")
        }
    } else {
        System.out.printf(
            "Error source %s, reason %s, message: %s%n", context.errorSource,
            reason, context.exception
        )
    }
}