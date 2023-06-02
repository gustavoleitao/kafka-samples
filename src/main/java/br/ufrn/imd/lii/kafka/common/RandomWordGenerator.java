package br.ufrn.imd.lii.kafka.common;

import java.util.Random;

public class RandomWordGenerator {
    private static final int MIN_LENGTH = 3;
    private static final int MAX_LENGTH = 8;
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

    public static String generateRandomWord() {
        Random random = new Random();
        int length = random.nextInt(MAX_LENGTH - MIN_LENGTH + 1) + MIN_LENGTH;
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(ALPHABET.length());
            char randomChar = ALPHABET.charAt(randomIndex);
            sb.append(randomChar);
        }

        return sb.toString();
    }
}