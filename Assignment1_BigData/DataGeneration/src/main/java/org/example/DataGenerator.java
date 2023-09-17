package org.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataGenerator {
    public static void main(String[] args) {
        int buyerCount = 10000;
        int purchaseCount = 1000000;
        String buyersFile = "buyers.txt";
        String purchasesFile = "purchases.txt";

        // Generate buyers dataset
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(buyersFile))) {
            for (int i = 1; i <= buyerCount; i++) {
                String name = generateName(10, 15);
                int age = generateAge(12, 75);
                String gender = generateGender();
                float salary = generateSalary(3500, 11000);
                bw.write(i + "," + name + "," + age + "," + gender + "," + salary + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Generate purchases dataset
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(purchasesFile))) {
            for (int i = 1; i <= purchaseCount; i++) {
                int buyerID = generateBuyerID(1, buyerCount);
                float price = generatePrice(10, 100);
                int numItems = generateNumItems(1, 10);
                bw.write(i + "," + buyerID + "," + price + "," + numItems + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String generateName(int minLength, int maxLength) {
        Random r = new Random();
        int length = r.nextInt(maxLength - minLength + 1) + minLength;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char c = (char) (r.nextInt(26) + 'a');
            sb.append(c);
        }
        return sb.toString();
    }

    private static int generateAge(int minAge, int maxAge) {
        Random r = new Random();
        return r.nextInt(maxAge - minAge + 1) + minAge;
    }

    private static String generateGender() {
        Random r = new Random();
        return r.nextInt(2) == 0 ? "male" : "female";
    }

    private static float generateSalary(float minSalary, float maxSalary) {
        Random r = new Random();
        return minSalary + r.nextFloat() * (maxSalary - minSalary);
    }

    private static int generateBuyerID(int minID, int maxID) {
        Random r = new Random();
        return r.nextInt(maxID - minID + 1) + minID;
    }

    private static float generatePrice(float minPrice, float maxPrice) {
        Random r = new Random();
        return minPrice + r.nextFloat() * (maxPrice - minPrice);
    }

    private static int generateNumItems(int minItems, int maxItems) {
        Random r = new Random();
        return r.nextInt(maxItems - minItems + 1) + minItems;
    }
}