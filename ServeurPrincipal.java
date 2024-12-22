import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ServeurPrincipal {

    private static List<StorageServerInfo> storageServers = Collections.synchronizedList(new ArrayList<>());
    private static final String LOG_FILE = "ServeurPrincipal.log";
    private static final String CONFIG_FILE = "listServer.conf";
    private static final int BROADCAST_PORT_START = 6001; // Début de la plage de ports pour envoyer les messages de diffusion
    private static final int BROADCAST_PORT_END = 6003; // Fin de la plage de ports pour envoyer les messages de diffusion
    private static final int RESPONSE_PORT_START = 6004; // Début de la plage de ports pour recevoir les réponses des serveurs secondaires
    private static final int RESPONSE_PORT_END = 6006; // Fin de la plage de ports pour recevoir les réponses des serveurs secondaires
    private static final int BROADCAST_INTERVAL = 30; // Intervalle en secondes
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Map<String, List<StorageServerInfo>> replicationInfo = new HashMap<>();

    public static void main(String[] args) {
        resetConfigFile(CONFIG_FILE); // Réinitialiser le fichier de configuration
        startServer();
        loadStorageServerConfig(CONFIG_FILE);
        startPeriodicBroadcast();
        listenForResponses(); // Démarrer l'écoute des réponses une seule fois
    }

    private static void log(String message) {
        String timestamp = dateFormat.format(new Date());
        try (FileWriter fw = new FileWriter(LOG_FILE, true);
             BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write(timestamp + " - " + message);
            bw.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void resetConfigFile(String configFilePath) {
        try (FileWriter fw = new FileWriter(configFilePath, false)) {
            // Ouvrir le fichier en mode écriture pour le vider
            log("Fichier de configuration réinitialisé.");
            System.out.println("Fichier de configuration réinitialisé.");
        } catch (IOException e) {
            log("Erreur lors de la réinitialisation du fichier de configuration : " + e.getMessage());
            System.out.println("Erreur lors de la réinitialisation du fichier de configuration : " + e.getMessage());
        }
    }

    private static void loadStorageServerConfig(String configFilePath) {
        synchronized (storageServers) {
            storageServers.clear(); // Clear the list before loading new configuration
            try (BufferedReader br = new BufferedReader(new FileReader(configFilePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(":");
                    if (parts.length == 2) {
                        String ip = parts[0];
                        int port = Integer.parseInt(parts[1]);
                        StorageServerInfo serverInfo = new StorageServerInfo(ip, port);
                        if (!storageServers.contains(serverInfo)) {
                            storageServers.add(serverInfo);
                            log("Configuration mise à jour avec : " + ip + ":" + port);
                            System.out.println("Configuration mise à jour avec : " + ip + ":" + port);
                        } else {
                            log("Configuration déjà présente : " + ip + ":" + port);
                            System.out.println("Configuration déjà présente : " + ip + ":" + port);
                        }
                    }
                }
                log("Configuration des serveurs de stockage chargée.");
                System.out.println("Configuration des serveurs de stockage chargée : " + storageServers.size() + " serveurs.");
                for (StorageServerInfo storageServerInfo : storageServers) {
                    System.out.println(storageServerInfo);
                }
            } catch (IOException e) {
                log("Erreur lors du chargement de la configuration : " + e.getMessage());
                System.out.println("Erreur lors du chargement de la configuration : " + e.getMessage());
            }
        }
    }

    private static void startServer() {
        new Thread(() -> {
            System.out.println("new thread");
            try (ServerSocket serverSocket = new ServerSocket(6000)) {
                log("Serveur principal démarré sur le port " + serverSocket.getLocalPort());

                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    new ClientHandler(clientSocket).start();
                }
            } catch (IOException e) {
                log("Erreur du serveur principal : " + e.getMessage());
            }
        }).start();
    }

    private static void startPeriodicBroadcast() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            sendBroadcastMessage();
        }, 0, BROADCAST_INTERVAL, TimeUnit.SECONDS);
    }

    private static void sendBroadcastMessage() {
        new Thread(() -> {
            for (int port = BROADCAST_PORT_START; port <= BROADCAST_PORT_END; port++) {
                try (DatagramSocket socket = new DatagramSocket()) {
                    socket.setBroadcast(true);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(baos);
                    dos.writeUTF("REQUEST_INFO");
                    byte[] buffer = baos.toByteArray();
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName("255.255.255.255"), port);
                    socket.send(packet);
                } catch (IOException e) {
                    // Handle exception silently
                }
            }
        }).start();
    }

    private static void listenForResponses() {
        for (int port = RESPONSE_PORT_START; port <= RESPONSE_PORT_END; port++) {
            final int currentPort = port;
            new Thread(() -> {
                try (DatagramSocket socket = new DatagramSocket(currentPort)) {
                    while (true) {
                        byte[] buffer = new byte[1024];
                        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                        socket.receive(packet);
                        new Thread(() -> {
                            try {
                                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(packet.getData(), 0, packet.getLength()));
                                String response = dis.readUTF();
                                storeConfig(response);
                            } catch (IOException e) {
                                // Handle exception silently
                            }
                        }).start();
                    }
                } catch (IOException e) {
                    // Handle exception silently
                }
            }).start();
        }
    }

    private static void storeConfig(String response) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(CONFIG_FILE));
            if (!lines.contains(response)) {
                try (FileWriter fw = new FileWriter(CONFIG_FILE, true);
                     BufferedWriter bw = new BufferedWriter(fw)) {
                    bw.write(response);
                    bw.newLine();
                    log("Configuration mise à jour avec : " + response);
                    System.out.println("Configuration mise à jour avec : " + response);
                }
            } else {
                log("Configuration déjà présente : " + response);
                System.out.println("Configuration déjà présente : " + response);
            }
            // Recharger la configuration après la mise à jour
            loadStorageServerConfig(CONFIG_FILE);
        } catch (IOException e) {
            log("Erreur lors de l'écriture dans le fichier de configuration : " + e.getMessage());
            System.out.println("Erreur lors de l'écriture dans le fichier de configuration : " + e.getMessage());
        }
    }

    private static void storeReplicationInfo(String fileName, String ip, int port) {
        StorageServerInfo serverInfo = new StorageServerInfo(ip, port);
        replicationInfo.computeIfAbsent(fileName, k -> new ArrayList<>()).add(serverInfo);
        log("Information de réplication stockée pour " + fileName + " : " + serverInfo);
    }

    static class ClientHandler extends Thread {
        private final Socket clientSocket;

        public ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try (
                DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
            ) {
                while (true) {
                    String operation = dis.readUTF();
                    log("Commande reçue : " + operation);
                    switch (operation) {
                        case "list":
                            listFiles(dos);
                            break;

                        case "upload":
                            receiveAndDistributeFile(dis, dos);
                            break;

                        case "download":
                            String fileName = dis.readUTF();
                            downloadFile(fileName, dos);
                            break;

                        case "DELETE_FILE":
                            String fileToDelete = dis.readUTF();
                            handleDeleteFile(fileToDelete, dos);
                            break;

                        case "REPLICATION_INFO":
                            String replicatedFileName = dis.readUTF();
                            String replicatedServerIp = dis.readUTF();
                            int replicatedServerPort = dis.readInt();
                            storeReplicationInfo(replicatedFileName, replicatedServerIp, replicatedServerPort);
                            dos.writeUTF("Replication info stored successfully");
                            break;

                        default:
                            dos.writeUTF("Commande non reconnue");
                            log("Commande non reconnue envoyée au client");
                            break;
                    }
                }
            } catch (IOException e) {
                log("Connexion client terminée : " + e.getMessage());
            }
        }

        private void receiveAndDistributeFile(DataInputStream dis, DataOutputStream dos) throws IOException {
            String fileName = dis.readUTF();
            long fileSize = dis.readLong();
            log("Réception du fichier : " + fileName + " de taille : " + fileSize);
            System.out.println("Réception du fichier : " + fileName + " de taille : " + fileSize);

            File tempFile = new File("temp_" + fileName);
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[4096];
                long totalRead = 0;
                int read;

                while (totalRead < fileSize && (read = dis.read(buffer)) != -1) {
                    fos.write(buffer, 0, read);
                    totalRead += read;
                    System.out.println("Réception en cours : " + totalRead + "/" + fileSize + " octets");
                }
            }

            log("Fichier reçu : " + fileName);
            System.out.println("Fichier reçu : " + fileName);

            int activeServers = storageServers.size();
            System.out.println("Nombre de serveurs actifs : " + activeServers);
            long minPartSize = 1 * 1024 * 1024; // 1 Mo en octets (ajusté pour des tests plus petits)
            int partCount = (int) Math.ceil((double) fileSize / minPartSize);

            // S'assurer que le nombre de parts ne dépasse pas le nombre de serveurs actifs
            partCount = Math.min(partCount, activeServers);

            // S'assurer que partCount est au moins 1
            partCount = Math.max(partCount, 1);
            System.out.println("Nombre de parties : " + partCount);

            List<File> parts = splitFile(tempFile, partCount);
            System.out.println("Fichier divisé en " + parts.size() + " parties.");

            for (int i = 0; i < parts.size(); i++) {
                if (i < storageServers.size()) {
                    sendFileToStorageServer(parts.get(i), storageServers.get(i), fileName);
                } else {
                    log("Pas assez de serveurs pour distribuer toutes les parties.");
                    System.out.println("Pas assez de serveurs pour distribuer toutes les parties.");
                }
            }

            tempFile.delete();
            for (File part : parts) {
                part.delete();
            }

            dos.writeUTF("Fichier distribué avec succès.");
            log("Message de confirmation envoyé au client");
            System.out.println("Message de confirmation envoyé au client");
        }

        private void updateFileMapping(String fileName, String partName, StorageServerInfo serverInfo) {
            try (FileWriter fw = new FileWriter("file_mapping.conf", true);
                 BufferedWriter bw = new BufferedWriter(fw)) {
                bw.write(fileName + "," + partName + "," + serverInfo.ip + ":" + serverInfo.port);
                bw.newLine();
                log("Fichier de suivi mis à jour pour " + partName);
            } catch (IOException e) {
                log("Erreur lors de la mise à jour du fichier de suivi : " + e.getMessage());
            }
        }

        private List<File> splitFile(File file, int partCount) throws IOException {
            List<File> parts = new ArrayList<>();
            long partSize = file.length() / partCount;
            long remainingBytes = file.length() % partCount;

            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
                for (int i = 0; i < partCount; i++) {
                    File partFile = new File(file.getName() + ".part" + i);
                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(partFile))) {
                        long bytesToWrite = partSize + (i < remainingBytes ? 1 : 0);
                        byte[] buffer = new byte[4096];
                        int bytesRead;
                        while (bytesToWrite > 0 && (bytesRead = bis.read(buffer, 0, (int) Math.min(buffer.length, bytesToWrite))) != -1) {
                            bos.write(buffer, 0, bytesRead);
                        }
                    }
                    parts.add(partFile);
                }
            }
            return parts;
        }

        private void sendFileToStorageServer(File part, StorageServerInfo serverInfo, String fileName) {
            try (Socket socket = new Socket(serverInfo.ip, serverInfo.port);
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                 FileInputStream fis = new FileInputStream(part)) {

                dos.writeUTF("store");
                dos.writeUTF(part.getName());
                dos.writeLong(part.length());

                byte[] buffer = new byte[4096];
                int read;
                while ((read = fis.read(buffer)) != -1) {
                    dos.write(buffer, 0, read);
                }

                log("Partie " + part.getName() + " envoyée à " + serverInfo);
                updateFileMapping(fileName, part.getName(), serverInfo);
            } catch (IOException e) {
                log("Erreur lors de l'envoi de " + part.getName() + " à " + serverInfo + " : " + e.getMessage());
            }
        }

        private void listFiles(DataOutputStream dos) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader("file_mapping.conf"))) {
                Set<String> files = new HashSet<>();
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 1) {
                        files.add(parts[0]);
                    }
                }
                dos.writeUTF("Fichiers disponibles : " + String.join(", ", files));
                log("Liste des fichiers envoyée au client");
            } catch (IOException e) {
                dos.writeUTF("Erreur lors de la lecture des fichiers disponibles : " + e.getMessage());
                log("Erreur lors de la lecture des fichiers disponibles : " + e.getMessage());
            }
        }

        private void downloadFile(String fileName, DataOutputStream dos) {
            try {
                File assembledFile = assembleFileFromParts(fileName);
                dos.writeUTF("Downloading:" + fileName);
                dos.writeLong(assembledFile.length());

                try (FileInputStream fis = new FileInputStream(assembledFile)) {
                    byte[] buffer = new byte[4096];
                    int read;
                    while ((read = fis.read(buffer)) != -1) {
                        dos.write(buffer, 0, read);
                    }
                }
                log("Fichier " + fileName+" téléchargé par le client");
            } catch (IOException e) {
                try {
                    dos.writeUTF("Erreur lors du téléchargement : " + e.getMessage());
                } catch (IOException ignored) {
                }
                log("Erreur lors du téléchargement du fichier : " + e.getMessage());
            }
        }

        private File assembleFileFromParts(String requestedFile) throws IOException {
            List<String> mappingLines = Files.readAllLines(Paths.get("file_mapping.conf"));
            List<String> partsToAssemble = new ArrayList<>();
            Map<String, String> partToServerMap = new HashMap<>();

            for (String line : mappingLines) {
                String[] tokens = line.split(",");
                if (tokens[0].equals(requestedFile)) {
                    partsToAssemble.add(tokens[1]);
                    partToServerMap.put(tokens[1], tokens[2]);
                }
            }

            if (partsToAssemble.isEmpty()) {
                throw new IOException("Aucune partie trouvée pour " + requestedFile);
            }

            File tempDir = new File("temp_parts");
            if (!tempDir.exists()) tempDir.mkdir();

            for (String part : partsToAssemble) {
                String serverInfo = partToServerMap.get(part);
                String[] serverDetails = serverInfo.split(":");
                String serverAddress = serverDetails[0];
                int serverPort = Integer.parseInt(serverDetails[1]);

                try {
                    downloadPart(serverAddress, serverPort, part, tempDir);
                } catch (IOException e) {
                    // Si le téléchargement échoue, essayer de télécharger à partir du serveur répliqué
                    List<StorageServerInfo> replicatedServers = replicationInfo.get(part);
                    if (replicatedServers != null) {
                        for (StorageServerInfo replicatedServer : replicatedServers) {
                            try {
                                downloadPart(replicatedServer.ip, replicatedServer.port, part, tempDir);
                                break;
                            } catch (IOException ignored) {
                            }
                        }
                    }
                }
            }

            File assembledFile = new File("assembled_files", requestedFile);
            if (!assembledFile.getParentFile().exists()) {
                assembledFile.getParentFile().mkdirs();
            }

            try (FileOutputStream fos = new FileOutputStream(assembledFile)) {
                for (String part : partsToAssemble) {
                    File partFile = new File(tempDir, part);
                    Files.copy(partFile.toPath(), fos);
                }
            }

            return assembledFile;
        }

        private void downloadPart(String serverAddress, int serverPort, String partName, File tempDir) throws IOException {
            if (!tempDir.exists()) {
                tempDir.mkdirs();
            }

            try (Socket socket = new Socket(serverAddress, serverPort);
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                 DataInputStream dis = new DataInputStream(socket.getInputStream());
                 FileOutputStream fos = new FileOutputStream(new File(tempDir, partName))) {

                dos.writeUTF("GET_PART");
                dos.writeUTF(partName);

                String response = dis.readUTF();
                if ("PART_FOUND".equals(response)) {
                    long fileSize = dis.readLong();

                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    long totalRead = 0;

                    while (totalRead < fileSize && (bytesRead = dis.read(buffer)) != -1) {
                        fos.write(buffer, 0, bytesRead);
                        totalRead += bytesRead;
                    }
                } else {
                    log("Erreur: Partie " + partName + " non trouvée sur " + serverAddress + ":" + serverPort);
                }
            }
        }

        private void deletePartFromSecondaryServer(String partFileName, String serverAddress) {
            String[] addressParts = serverAddress.split(":");
            if (addressParts.length == 2) {
                String ip = addressParts[0];
                int port = Integer.parseInt(addressParts[1]);

                try (Socket socket = new Socket(ip, port);
                     DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                     DataInputStream dis = new DataInputStream(socket.getInputStream())) {

                    dos.writeUTF("DELETE_PART");
                    dos.writeUTF(partFileName);

                    String response = dis.readUTF();
                    if ("SUCCESS".equals(response)) {
                        log("Partie supprimée avec succès : " + partFileName + " sur " + serverAddress);
                    } else {
                        log("Échec de la suppression de la partie : " + partFileName + " sur " + serverAddress);
                    }
                } catch (IOException e) {
                    log("Erreur lors de la connexion au serveur secondaire (" + serverAddress + ") : " + e.getMessage());
                }
            }
        }

        private void handleDeleteFile(String fileName, DataOutputStream dos) {
            File mappingFile = new File("file_mapping.conf");
            File tempFile = new File("file_mapping_temp.txt");

            boolean fileFound = false;

            try (
                BufferedReader reader = new BufferedReader(new FileReader(mappingFile));
                BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))
            ) {
                String line;

                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 3 && parts[0].trim().equals(fileName.trim())) {
                        fileFound = true;
                        String partFileName = parts[1].trim();
                        String serverAddress = parts[2].trim();
                        deletePartFromSecondaryServer(partFileName, serverAddress);
                    } else {
                        writer.write(line);
                        writer.newLine();
                    }
                }
            } catch (IOException e) {
                try {
                    dos.writeUTF("Erreur lors de la suppression : " + e.getMessage());
                } catch (IOException ioException) {
                    log("Erreur lors de l'envoi du message d'erreur au client : " + ioException.getMessage());
                }
                log("Erreur lors du traitement du mapping : " + e.getMessage());
                return;
            }

            if (fileFound) {
                if (mappingFile.delete() && tempFile.renameTo(mappingFile)) {
                    try {
                        dos.writeUTF("SUCCESS");
                        log("Fichier supprimé avec succès : " + fileName);
                    } catch (IOException e) {
                        log("Erreur lors de l'envoi de SUCCESS : " + e.getMessage());
                    }
                } else {
                    try {
                        dos.writeUTF("Erreur lors de la mise à jour du fichier de mapping.");
                    } catch (IOException e) {
                        log("Erreur lors de l'envoi du message au client : " + e.getMessage());
                    }
                }
            } else {
                try {
                    dos.writeUTF("Fichier introuvable dans le mapping.");
                } catch (IOException e) {
                    log("Erreur lors de l'envoi du message d'erreur au client : " + e.getMessage());
                }
            }
        }
    }

    static class StorageServerInfo {
        String ip;
        int port;

        public StorageServerInfo(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        @Override
        public String toString() {
            return ip + ":" + port;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != getClass()) return false;
            StorageServerInfo that = (StorageServerInfo) obj;
            return port == that.port && Objects.equals(ip, that.ip);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip, port);
        }
    }
}
