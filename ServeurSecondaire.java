import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ServeurSecondaire {
    private static int port = 5004;  // Le port sur lequel le serveur secondaire écoute pour recevoir des fichiers
    private static final String LOG_FILE = "ServeurSecondaire.log";
    private static final int BROADCAST_PORT = 6002; // Le port sur lequel le serveur secondaire écoute pour les messages de diffusion
    private static int responsePort = 6005; // Le port sur lequel le serveur secondaire envoie les informations de connexion
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String otherServerIp = "127.0.0.1"; // Adresse IP de l'autre serveur secondaire
    private static final int otherServerPort = 5003;
    private static final Set<String> replicatedFiles = Collections.synchronizedSet(new HashSet<>()); // Ensemble pour suivre les fichiers répliqués

    public static void main(String[] args) {
        if (args.length >= 3) {
            port = Integer.parseInt(args[0]);
            responsePort = Integer.parseInt(args[2]);
        }

        startServer();
        listenForBroadcast();
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

    private static void startServer() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                log("Serveur secondaire démarré sur le port " + port + ".");

                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    log("Connexion acceptée de " + clientSocket.getInetAddress());
                    new FileReceiveHandler(clientSocket).start();
                }
            } catch (IOException e) {
                log("Erreur du serveur secondaire : " + e.getMessage());
            }
        }).start();
    }

    private static void listenForBroadcast() {
        new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket(BROADCAST_PORT)) {
                byte[] buffer = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                while (true) {
                    socket.receive(packet);
                    new Thread(() -> {
                        try {
                            String message = new DataInputStream(new ByteArrayInputStream(packet.getData(), 0, packet.getLength())).readUTF();
                            log("Message de diffusion reçu : " + message);
                            if ("REQUEST_INFO".equals(message)) {
                                sendResponse(packet.getAddress(), port);
                            }
                        } catch (IOException e) {
                            log("Erreur lors de la réception du message de diffusion : " + e.getMessage());
                        }
                    }).start();
                }
            } catch (IOException e) {
                log("Erreur lors de la réception du message de diffusion : " + e.getMessage());
            }
        }).start();
    }

    private static void sendResponse(InetAddress address, int port) {
        new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket()) {
                String response = InetAddress.getLocalHost().getHostAddress() + ":" + port;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeUTF(response);
                byte[] buffer = baos.toByteArray();
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, responsePort);
                socket.send(packet);
                log("Réponse envoyée : " + response);
            } catch (IOException e) {
                log("Erreur lors de l'envoi de la réponse : " + e.getMessage());
            }
        }).start();
    }

    static class FileReceiveHandler extends Thread {
        private Socket clientSocket;

        public FileReceiveHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try (
                DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
            ) {
                String command = dis.readUTF();
                log("Commande reçue : " + command);

                if ("store".equals(command)) {
                    receiveFile(dis, dos);
                } else if ("GET_PART".equals(command)) {
                    handleGetPart(dis, dos);
                } else if ("DELETE_PART".equals(command)) {
                    handleDeletePart(dis, dos);
                } else {
                    dos.writeUTF("Commande non reconnue");
                    log("Commande non reconnue envoyée au client");
                }

            } catch (IOException e) {
                log("Erreur avec le client : " + e.getMessage());
            }
        }

        private void receiveFile(DataInputStream dis, DataOutputStream dos) {
            try {
                String fileName = dis.readUTF();
                long fileSize = dis.readLong();
                log("Réception du fichier : " + fileName + " de taille : " + fileSize);

                File file = new File("storage/" + fileName);
                file.getParentFile().mkdirs();

                try (FileOutputStream fos = new FileOutputStream(file)) {
                    byte[] buffer = new byte[4096];
                    long totalRead = 0;
                    int bytesRead;

                    while (totalRead < fileSize && (bytesRead = dis.read(buffer)) != -1) {
                        fos.write(buffer, 0, bytesRead);
                        totalRead += bytesRead;
                    }
                }

                log("Fichier " + fileName + " reçu et sauvegardé.");
                dos.writeUTF("Fichier reçu et sauvegardé avec succès.");

                // Réplication
                if (!replicatedFiles.contains(fileName)) {
                    replicateFile(fileName, fileSize, otherServerIp, otherServerPort);
                    replicatedFiles.add(fileName); // Marquer la réplication comme effectuée
                }

            } catch (IOException e) {
                try {
                    dos.writeUTF("Erreur lors de la réception du fichier : " + e.getMessage());
                } catch (IOException ioException) {
                    log("Erreur lors de l'envoi du message d'erreur au client : " + ioException.getMessage());
                }
                log("Erreur lors de la réception du fichier : " + e.getMessage());
            }
        }

        private void replicateFile(String fileName, long fileSize, String otherServerIp, int otherServerPort) {
            // Choisir un autre serveur secondaire pour la réplication

            if (otherServerIp.equals("") || otherServerPort == 0) {
                log("Adresse IP ou port du serveur secondaire non défini pour la réplication");
                return;
            }

            System.out.println("Début de la réplication du fichier : " + fileName + " vers " + otherServerIp + ":" + otherServerPort);

            try (Socket socket = new Socket(otherServerIp, otherServerPort);
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                 FileInputStream fis = new FileInputStream("storage/" + fileName)) {

                dos.writeUTF("store");
                dos.writeUTF(fileName);
                dos.writeLong(fileSize);

                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    dos.write(buffer, 0, bytesRead);
                }

                System.out.println("Fichier " + fileName + " répliqué à " + otherServerIp + ":" + otherServerPort);
                log("Fichier " + fileName + " répliqué à " + otherServerIp + ":" + otherServerPort);

                // Informer le serveur principal de la réplication
                informPrincipal(fileName, otherServerIp, otherServerPort);

            } catch (IOException e) {
                System.out.println("Erreur lors de la réplication du fichier : " + e.getMessage());
                log("Erreur lors de la réplication du fichier : " + e.getMessage());
            }
        }

        private void informPrincipal(String fileName, String otherServerIp, int otherServerPort) {
            try (Socket socket = new Socket("127.0.0.1", 6000); // Adresse IP et port du serveur principal
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

                dos.writeUTF("REPLICATION_INFO");
                dos.writeUTF(fileName);
                dos.writeUTF(otherServerIp);
                dos.writeInt(otherServerPort);

                System.out.println("Information de réplication envoyée au serveur principal pour le fichier : " + fileName);
                log("Information de réplication envoyée au serveur principal pour le fichier : " + fileName);

            } catch (IOException e) {
                System.out.println("Erreur lors de l'envoi de l'information de réplication au serveur principal : " + e.getMessage());
                log("Erreur lors de l'envoi de l'information de réplication au serveur principal : " + e.getMessage());
            }
        }

        private void handleGetPart(DataInputStream dis, DataOutputStream dos) throws IOException {
            String partName = dis.readUTF();
            log("Demande de partie reçue : " + partName);
            File partFile = new File("storage", partName);

            if (partFile.exists() && partFile.isFile()) {
                dos.writeUTF("PART_FOUND");
                dos.writeLong(partFile.length());
                log("Partie trouvée et taille envoyée : " + partName);

                try (FileInputStream fis = new FileInputStream(partFile)) {
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = fis.read(buffer)) != -1) {
                        dos.write(buffer, 0, bytesRead);
                    }
                }
                log("Partie " + partName + " envoyée au client");
            } else {
                dos.writeUTF("PART_NOT_FOUND");
                log("Partie non trouvée : " + partName);
            }
        }

        private void handleDeletePart(DataInputStream dis, DataOutputStream dos) throws IOException {
            String partName = dis.readUTF();
            log("Demande de suppression de partie reçue : " + partName);
            File partFile = new File("storage/" + partName);

            if (partFile.exists() && partFile.delete()) {
                dos.writeUTF("SUCCESS");
                log("Partie supprimée avec succès : " + partName);
            } else {
                dos.writeUTF("FAILURE");
                log("Échec de la suppression de la partie : " + partName);
            }
        }
    }
}
