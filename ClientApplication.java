import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.*;

public class ClientApplication {
    private JFrame frame;
    private JTextField ipField, portField;
    private JButton connectButton, listFilesButton, uploadButton, downloadButton, deleteButton;
    private JList<String> fileList;
    private DefaultListModel<String> listModel;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private JProgressBar progressBar;

    public static void main(String[] args) {
        EventQueue.invokeLater(() -> {
            try {
                ClientApplication window = new ClientApplication();
                window.frame.setVisible(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public ClientApplication() {
        initialize();
    }

    private void initialize() {
        frame = new JFrame("Client Application");
        frame.setBounds(100, 100, 450, 300);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().setLayout(new BorderLayout(0, 0));

        JPanel panel = new JPanel();
        frame.getContentPane().add(panel, BorderLayout.NORTH);
        panel.setLayout(new GridLayout(3, 2));

        panel.add(new JLabel("Server IP:"));
        ipField = new JTextField();
        panel.add(ipField);
        ipField.setColumns(10);

        panel.add(new JLabel("Server Port:"));
        portField = new JTextField();
        panel.add(portField);
        portField.setColumns(10);

        connectButton = new JButton("Connect");
        panel.add(connectButton);

        listFilesButton = new JButton("List Files");
        panel.add(listFilesButton);

        JPanel filePanel = new JPanel();
        frame.getContentPane().add(filePanel, BorderLayout.CENTER);
        filePanel.setLayout(new BorderLayout(0, 0));

        listModel = new DefaultListModel<>();
        fileList = new JList<>(listModel);
        filePanel.add(new JScrollPane(fileList), BorderLayout.CENTER);

        JPanel buttonPanel = new JPanel();
        frame.getContentPane().add(buttonPanel, BorderLayout.SOUTH);
        buttonPanel.setLayout(new BorderLayout());

        JPanel actionButtonPanel = new JPanel();
        actionButtonPanel.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));
        buttonPanel.add(actionButtonPanel, BorderLayout.NORTH);

        uploadButton = new JButton("Upload");
        actionButtonPanel.add(uploadButton);

        downloadButton = new JButton("Download");
        actionButtonPanel.add(downloadButton);

        deleteButton = new JButton("Delete");
        actionButtonPanel.add(deleteButton);

        // Ajouter la barre de progression
        progressBar = new JProgressBar(0, 100);
        progressBar.setStringPainted(true);
        buttonPanel.add(progressBar, BorderLayout.SOUTH);

        // Action listeners
        connectButton.addActionListener(this::connectToServer);
        listFilesButton.addActionListener(this::listFiles);
        uploadButton.addActionListener(this::uploadFile);
        downloadButton.addActionListener(this::downloadFile);
        deleteButton.addActionListener(this::deleteFile);
    }
    private void connectToServer(ActionEvent e) {
        String ip = ipField.getText();
        int port = Integer.parseInt(portField.getText());

        try {
            socket = new Socket(ip, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            JOptionPane.showMessageDialog(frame, "Connected to server");
        } catch (IOException ex) {
            JOptionPane.showMessageDialog(frame, "Connection failed: " + ex.getMessage());
        }
    }
    private void listFiles(ActionEvent e) {
        if (socket == null || socket.isClosed()) {
            JOptionPane.showMessageDialog(frame, "Not connected to any server.");
            return;
        }
    
        try {
            // Envoyer la commande "list" au serveur
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeUTF("list");
            dos.flush(); // Toujours vider après écriture
            System.out.println("Commande 'list' envoyée.");
    
            // Recevoir la réponse
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            String response = dis.readUTF(); // Lire la réponse en UTF
            System.out.println("Réponse reçue : " + response);
    
            listModel.clear();
            if (response.startsWith("Fichiers disponibles")) {
                // Extraire les fichiers de la réponse
                String[] files = response.split(":")[1].trim().split(", ");
                for (String file : files) {
                    listModel.addElement(file);
                }
            } else {
                JOptionPane.showMessageDialog(frame, response); // Afficher une éventuelle erreur
            }
        } catch (IOException ex) {
            JOptionPane.showMessageDialog(frame, "Error listing files: " + ex.getMessage());
        }
    }
    
    private void uploadFile(ActionEvent e) {
        if (socket == null || socket.isClosed()) {
            JOptionPane.showMessageDialog(frame, "Not connected to any server.");
            return;
        }
    
        JFileChooser fileChooser = new JFileChooser();
        int result = fileChooser.showOpenDialog(frame);
        if (result == JFileChooser.APPROVE_OPTION) {
            File file = fileChooser.getSelectedFile();
            new Thread(() -> {
                try {
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
    
                    System.out.println("Début de l'upload du fichier : " + file.getName());
                    dos.writeUTF("upload");
                    dos.writeUTF(file.getName());
                    dos.writeLong(file.length());
    
                    try (FileInputStream fileIn = new FileInputStream(file)) {
                        byte[] buffer = new byte[4096];
                        long totalRead = 0;
                        int bytesRead;
                        progressBar.setValue(0);
                        progressBar.setMaximum((int) file.length());
    
                        while ((bytesRead = fileIn.read(buffer)) != -1) {
                            dos.write(buffer, 0, bytesRead);
                            totalRead += bytesRead;
                            progressBar.setValue((int) totalRead);
                            System.out.println("Upload en cours : " + totalRead + "/" + file.length() + " octets");
                        }
                    }
                    System.out.println("Upload terminé pour le fichier : " + file.getName());
                    JOptionPane.showMessageDialog(frame, "File uploaded successfully!");
                    listFiles(null); // Mettre à jour la liste des fichiers après l'upload
                } catch (IOException ex) {
                    System.out.println("Erreur lors de l'upload du fichier : " + ex.getMessage());
                    JOptionPane.showMessageDialog(frame, "Error uploading file: " + ex.getMessage());
                }
            }).start();
        }
    }
    
    private void downloadFile(ActionEvent e) {
        if (socket == null || socket.isClosed()) {
            JOptionPane.showMessageDialog(frame, "Not connected to any server.");
            return;
        }
    
        String selectedFile = fileList.getSelectedValue();
        if (selectedFile == null) {
            JOptionPane.showMessageDialog(frame, "No file selected.");
            return;
        }
    
        new Thread(() -> {
            try {
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                DataInputStream dis = new DataInputStream(socket.getInputStream());
    
                dos.writeUTF("download");
                dos.writeUTF(selectedFile);
    
                String response = dis.readUTF();
                if (response.startsWith("Downloading")) {
                    String fileName = response.split(":")[1];
                    long fileLength = dis.readLong();
    
                    JFileChooser fileChooser = new JFileChooser();
                    fileChooser.setSelectedFile(new File(fileName));
                    int result = fileChooser.showSaveDialog(frame);
                    if (result == JFileChooser.APPROVE_OPTION) {
                        File file = fileChooser.getSelectedFile();
    
                        try (FileOutputStream fileOut = new FileOutputStream(file)) {
                            byte[] buffer = new byte[4096];
                            long totalRead = 0;
                            int bytesRead;
                            progressBar.setValue(0);
                            progressBar.setMaximum((int) fileLength);
    
                            while (totalRead < fileLength &&
                                    (bytesRead = dis.read(buffer, 0, (int) Math.min(buffer.length, fileLength - totalRead))) != -1) {
                                fileOut.write(buffer, 0, bytesRead);
                                totalRead += bytesRead;
                                progressBar.setValue((int) totalRead);
                            }
                        }
    
                        JOptionPane.showMessageDialog(frame, "File downloaded successfully!");
                    }
                } else {
                    JOptionPane.showMessageDialog(frame, response);
                }
            } catch (IOException ex) {
                JOptionPane.showMessageDialog(frame, "Error: " + ex.getMessage());
            }
        }).start();
    }

    private void deleteFile(ActionEvent e) {
        String selectedFile = fileList.getSelectedValue();
        if (selectedFile == null) {
            JOptionPane.showMessageDialog(frame, "Veuillez sélectionner un fichier à supprimer.", "Erreur", JOptionPane.ERROR_MESSAGE);
            return;
        }
    
        new Thread(() -> {
            try (Socket socket = new Socket(ipField.getText(), Integer.parseInt(portField.getText()));
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                 DataInputStream dis = new DataInputStream(socket.getInputStream())) {
    
                dos.writeUTF("DELETE_FILE");
                dos.writeUTF(selectedFile);
    
                String response = dis.readUTF();
                if ("SUCCESS".equals(response)) {
                    JOptionPane.showMessageDialog(frame, "Fichier supprimé avec succès.", "Succès", JOptionPane.INFORMATION_MESSAGE);
                    listModel.removeElement(selectedFile);
                } else {
                    JOptionPane.showMessageDialog(frame, "Erreur : " + dis.readUTF(), "Erreur", JOptionPane.ERROR_MESSAGE);
                }
            } catch (IOException ex) {
                JOptionPane.showMessageDialog(frame, "Erreur de connexion : " + ex.getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
            }
        }).start();
    }
}
