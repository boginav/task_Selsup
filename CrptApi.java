package by.bogin;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.SneakyThrows;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class CrptApi {

    private Semaphore semaphore;
    private static String CREATE_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";

    private TimeUnit timeUnit;

    private ScheduledExecutorService scheduledExecutorService;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.timeUnit = timeUnit;
        int capacity = requestLimit > 0 ? requestLimit : 1;
        semaphore = new Semaphore(capacity);
        scheduledExecutorService = Executors.newScheduledThreadPool(capacity);

    }


    public void createDocument(CreateDocumentObject document, String signature) throws Exception {
        semaphore.acquire(1);

        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            semaphore.release();
        }, 1, 1, timeUnit);

        System.out.println("Start creation of document at " + System.currentTimeMillis());
        sendCreateRequest(document, signature);

    }


    private void sendCreateRequest(Object document, String signature) throws Exception {
        URL url = new URL(CREATE_URL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);

        String jsonInputString = convertToJson(document, signature);
        connection.getOutputStream().write(jsonInputString.getBytes(StandardCharsets.UTF_8));

        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            System.out.println("Failed : HTTP error code : " + responseCode);
            //throw new RuntimeException("Failed : HTTP error code : " + responseCode);
        }
    }

    private String convertToJson(Object document, String signature) throws JsonProcessingException {
        //todo не совсем понятно где использовать подпись , поэтому оставил  пока как заглушку
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String json = objectMapper.writeValueAsString(document);
        return json;
    }


    @Data
    public static class CreateDocumentObject {
        @Data
        public static class Description {
            private String participantInn;
        }

        public enum DocTepeEnum {
            LP_INTRODUCE_GOODS
        }

        private static class Product {
            private String certificate_document;
            @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
            private Date certificate_document_date;
            private String certificate_document_number;
            private String owner_inn;
            private String producer_inn;
            @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
            private Date production_date;
            private String tnved_code;
            private String uit_code;
            private String uitu_code;
        }

        private Description description;
        private String doc_id;
        private String doc_status;
        private DocTepeEnum doc_type;
        private Boolean importRequest;
        private String owner_inn;
        private String participant_inn;
        private String producer_inn;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private Date production_date;
        private String production_type;
        private List<Product> products;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private Date reg_date;
        private String reg_number;
    }


    public static void main(String[] args) {
        CrptApi crptApi = new CrptApi(TimeUnit.MINUTES, 5);

        Thread[] threads = new Thread[10];

        for (int i = 0; i < 10; i++) {
            threads[i] = new Thread(new Task(i, crptApi));
            threads[i].start();
        }

        // wait when all threads are finished
        for (int i = 0; i < 10; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("All threads are finished.");
    }


    static class Task implements Runnable {
        private final int threadNumber;
        private final CrptApi crptApi;


        public Task(int threadNumber, CrptApi crptApi) {
            this.threadNumber = threadNumber;
            this.crptApi = crptApi;
        }

        @SneakyThrows
        @Override
        public void run() {
            System.out.println("Thread " + threadNumber + " started.");

            CrptApi.CreateDocumentObject document = new CrptApi.CreateDocumentObject();
            CrptApi.CreateDocumentObject.Description description = new CrptApi.CreateDocumentObject.Description();
            description.setParticipantInn("ParticipantInn");
            document.setDescription(description);
            document.setDoc_type(CrptApi.CreateDocumentObject.DocTepeEnum.LP_INTRODUCE_GOODS);
            document.setReg_date(new Date());

            crptApi.createDocument(document, "sign");

            System.out.println("Thread " + threadNumber + " complete.");
        }
    }

}