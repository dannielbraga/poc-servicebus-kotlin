package gb.tech

import com.azure.messaging.servicebus.*
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

fun main() {

    val connectionString = "Endpoint=sb://veripag-split-br-so.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=oDUiyFd6ncFj72HneCcdPhve10hVX6dsYzsgayR6D1s="
    val topicName = "sb-topic-sale"

    val producerClient = ServiceBusClientBuilder()
        .connectionString(connectionString)
        .sender()
        .topicName(topicName)
        .buildClient()

    val message = readJson("C:\\Users\\danni\\IdeaProjects\\poc-servicebus-kotlin\\src\\main\\kotlin\\transaction.json")

    val serviceBusMessage = ServiceBusMessage(message)
    serviceBusMessage.applicationProperties["api_key"] = "ak_live_1e0f7e899f8f1ef3871412ceb31575b309da4465"

    println("Messagem a ser enviada no topico $topicName: ${serviceBusMessage.body}\n com properties: ${serviceBusMessage.applicationProperties}")

    try {
        println("Enviando mensagem para Service Bus...")
        producerClient.sendMessage(serviceBusMessage)
        println("Mensagem enviada com sucesso!")
    } catch (ex: Exception) {
        println("Erro ao enviar mensagem: ${ex.message}")
    } finally {
        producerClient.close()
    }
}

@Throws(IOException::class)
fun readJson(path: String?): String {
    val json = java.lang.String.join(
        " ",
        Files.readAllLines(
            Paths.get(path),
            StandardCharsets.UTF_8
        )
    )
    return json
}
