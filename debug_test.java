import java.util.*;

public class debug_test {
    public static void main(String[] args) {
        // Test filename-based ordering
        String filename1 = "z-file.parquet";  // Should come AFTER with filename ordering
        String filename2 = "a-file.parquet";  // Should come FIRST with filename ordering

        System.out.println("Filename comparison:");
        System.out.println("z-file.parquet vs a-file.parquet: " + filename1.compareTo(filename2));
        System.out.println("a-file.parquet vs z-file.parquet: " + filename2.compareTo(filename1));

        // Test long comparison
        long minSeq1 = 98L;  // Should come FIRST with minSeq ordering
        long minSeq2 = 99L;  // Should come SECOND with minSeq ordering

        System.out.println("\nMinSeq comparison:");
        System.out.println("98L vs 99L: " + Long.compare(minSeq1, minSeq2));
        System.out.println("99L vs 98L: " + Long.compare(minSeq2, minSeq1));

        // Test TreeSet behavior
        TreeSet<String> filenameOrdered = new TreeSet<>((a, b) -> a.compareTo(b));
        filenameOrdered.add(filename1);
        filenameOrdered.add(filename2);

        System.out.println("\nTreeSet with filename ordering:");
        for (String f : filenameOrdered) {
            System.out.println(f);
        }
    }
}
