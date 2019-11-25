package com.aws.datagen;

import com.aws.datagen.model.Contribution;
import com.aws.datagen.model.Customer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.text.WordUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class DataGenerator
{
    private static String maleFirstNamesLocation = "data/male_names.txt";
    private static String femaleFirstNamesLocation = "data/female_names.txt";
    private static String lastNamesLocation = "data/last_names.txt";

    private FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd");

    private final Date minDate;
    private final Date maxDate;

    private final long maxDays;

    private final RandomDataGenerator  dataGenerator = new RandomDataGenerator();
    private final Random random = new Random();
    private final long ONE_DAY = 1000L * 60L * 60L * 24L;

    /**
     * 100MB default row group size
     */
    private int rowGroupSize = 1024 * 1024 * 100;

    /**
     * The range of data to simulate
     */
    private int startYear = 2002;
    private int endYear = 2011;

    /**
     * Min average salaries in the start year
     */
    private double femaleAverageSalary = 22000.0;
    private double maleAverageSalary = 36000.0;

    /**
     * Salary growth per year
     */
    private double femaleGrowthRate = 0.1211;
    private double maleGrowthRate = 0.1355;

    /**
     * The maximum salary variance from the average salary
     */
    private double salaryVariance = 0.4;

    /**
     * Yearly and monthly contribution rates
     */
    private double yearlyContrib = 0.09;
    private double monthlyContrib = yearlyContrib / 12.0;

    public DataGenerator() throws ParseException
    {
        minDate = dateFormat.parse(String.format("%d-01-01", startYear));
        maxDate = dateFormat.parse(String.format("%d-12-31", endYear));
        maxDays = ChronoUnit.DAYS.between(minDate.toInstant(), maxDate.toInstant());
    }

    public void createContributions(List<Customer> customers) throws IOException
    {
        System.out.println("Creating contributions");

        String outputCSVPath = "";
        String outputCompressedCSVPath = "";
        String outputParquetPath = "";

        FastDateFormat yearFormat = FastDateFormat.getInstance("yyyy");
        FastDateFormat yearMonthFormat = FastDateFormat.getInstance("yyyy-MM");

        List<List<Customer>> customersPerPayDate = new ArrayList<>();

        for (int i = 0; i < 32; i++)
        {
            customersPerPayDate.add(new ArrayList<>());
        }

        for (Customer customer: customers)
        {
            LocalDate localPayDate = customer.getJoinDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            int day = localPayDate.getDayOfMonth();
            customersPerPayDate.get(day).add(customer);
        }

        Date payDate = minDate;

        long contributionId = 0L;

        BufferedWriter writer = null;
        CSVPrinter csvPrinter = null;
        OutputStreamWriter compressedWriter = null;
        CSVPrinter compressedCSVPrinter = null;
        GZIPOutputStream gzipStream = null;
        ParquetWriter<Contribution> parquetWriter = null;

        while (payDate.getTime() <= maxDate.getTime())
        {
            LocalDate localPayDate = payDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

            int payDay = localPayDate.getDayOfMonth();
            int payYear = localPayDate.getYear();

            String csvPath = String.format("output_csv/contributions/contributionYear=%s/contributionMonth=%s/contributions_%s.csv",
                    yearFormat.format(payDate), yearMonthFormat.format(payDate),
                    yearMonthFormat.format(payDate));

            String compressedCSVPath = String.format("output_compressed/contributions/contributionYear=%s/contributionMonth=%s/contributions_%s.csv.gz",
                    yearFormat.format(payDate), yearMonthFormat.format(payDate),
                    yearMonthFormat.format(payDate));

            String parquetPath = String.format("output_parquet/contributions/contributionYear=%s/contributionMonth=%s/contributions_%s.snappy.parquet",
                    yearFormat.format(payDate), yearMonthFormat.format(payDate),
                    yearMonthFormat.format(payDate));

            if (!parquetPath.equals(outputParquetPath))
            {
                if (parquetWriter != null)
                {
                    parquetWriter.close();
                }

                File outputFile = new File(parquetPath);
                outputFile.getParentFile().mkdirs();

                parquetWriter = AvroParquetWriter.<Contribution>builder(new Path(parquetPath))
                                .withSchema(Contribution.SCHEMA)
                                .withCompressionCodec(CompressionCodecName.SNAPPY)
                                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                                .withRowGroupSize(rowGroupSize)
                                .build();

                outputParquetPath = parquetPath;
            }

            if (!csvPath.equals(outputCSVPath))
            {
                if (csvPrinter != null && writer != null)
                {
                    csvPrinter.flush();
                    csvPrinter.close();
                    writer.close();
                }

                File outputFile = new File(csvPath);
                outputFile.getParentFile().mkdirs();
                writer = Files.newBufferedWriter(Paths.get(outputFile.getAbsolutePath()));
                csvPrinter = new CSVPrinter(writer, CSVFormat.EXCEL.withQuote('"').withQuoteMode(QuoteMode.NON_NUMERIC)
                        .withHeader("ContributionId", "CustomerId", "ContributionDate", "Amount"));

                outputCSVPath = csvPath;
            }

            if (!compressedCSVPath.equals(outputCompressedCSVPath))
            {
                if (compressedCSVPrinter != null && compressedWriter != null)
                {
                    compressedCSVPrinter.flush();
                    compressedCSVPrinter.close();
                    compressedWriter.close();
                    gzipStream.close();
                }

                File outputFile = new File(compressedCSVPath);
                outputFile.getParentFile().mkdirs();

                gzipStream = new GZIPOutputStream(new FileOutputStream(outputFile));
                compressedWriter = new OutputStreamWriter(gzipStream);

                compressedCSVPrinter = new CSVPrinter(compressedWriter, CSVFormat.EXCEL.withQuote('"').withQuoteMode(QuoteMode.NON_NUMERIC)
                        .withHeader("ContributionId", "CustomerId", "ContributionDate", "Amount"));

                outputCompressedCSVPath = compressedCSVPath;
            }

            for (Customer c: customersPerPayDate.get(payDay))
            {
                if (payDate.getTime() < c.getJoinDate().getTime())
                {
                    continue;
                }

                double averageSalary = c.getMale() ? maleAverageSalary : femaleAverageSalary;
                double growthRate = c.getMale() ? maleGrowthRate : femaleGrowthRate;

                double salary = c.getSalary(startYear, payYear, averageSalary, growthRate);
                double amount = salary * monthlyContrib;

                amount = Double.parseDouble(String.format("%.2f", amount));

                Contribution contribution = new Contribution(contributionId++, c.getCustomerId());
                contribution.setAmount(amount);
                contribution.setContributionDate(payDate);

                parquetWriter.write(contribution);

                csvPrinter.printRecord(contribution.getContributionId(), contribution.getCustomerId(),
                        dateFormat.format(payDate), amount);

                compressedCSVPrinter.printRecord(contribution.getContributionId(), contribution.getCustomerId(),
                        dateFormat.format(payDate), amount);
            }

            payDate = new Date(payDate.getTime() + ONE_DAY);
        }

        csvPrinter.flush();
        writer.close();

        compressedCSVPrinter.flush();
        compressedWriter.close();
        gzipStream.close();

        parquetWriter.close();
    }

    public List<Customer> createCustomers(int count) throws IOException
    {
        System.out.println("Creating customers");

        List<String> femaleNames = loadFirstNames(femaleFirstNamesLocation);
        List<String> maleNames = loadFirstNames(maleFirstNamesLocation);
        List<String> lastNames = loadLastNames(lastNamesLocation);

        double maleWeight = 0.63;
        long customerId = 0;

        List<Customer> customers = new ArrayList<>();

        for (int i = 0; i < count; i++)
        {
            Customer customer = new Customer(customerId++);

            double variance = random.nextDouble() * salaryVariance;

            if (random.nextBoolean())
            {
                variance *= -1.0;
            }

            customer.setSalaryVariance(variance);

            Date joinDate = new Date(minDate.getTime() + dataGenerator.nextLong(0L, maxDays) * ONE_DAY);
            customer.setJoinDate(joinDate);

            boolean male = random.nextDouble() < maleWeight;

            customer.setMale(male);
            customer.setTitle(getTitle(male));

            if (male)
            {
                customer.setFirstName(makeName(maleNames));
            }
            else
            {
                customer.setFirstName(makeName(femaleNames));
            }

            customer.setLastName(makeName(lastNames));

            customers.add(customer);
        }

        return customers;
    }

    /**
     * Makes a name from a list of names
     * @param names the list of all names
     * @return a formatted name
     */
    private String makeName(List<String> names)
    {
        int index = dataGenerator.nextInt(1, names.size() - 1);
        String name = names.get(index);
        return WordUtils.capitalizeFully(name);
    }

    private List<String> loadFirstNames(String location) throws IOException
    {
        return IOUtils.readLines(new FileInputStream(location), "UTF-8");
    }

    private List<String> loadLastNames(String location) throws IOException
    {
        return IOUtils.readLines(new FileInputStream(location), "UTF-8");
    }

    private void saveCustomers(List<Customer> customers) throws IOException
    {
        System.out.println("Saving: " + customers.size() + " customers");

        File outputDir = new File("output_csv/customers");
        outputDir.mkdirs();

        outputDir = new File("output_compressed/customers");
        outputDir.mkdirs();

        outputDir = new File("output_parquet/customers");
        outputDir.mkdirs();

        BufferedWriter writer = Files.newBufferedWriter(Paths.get("output_csv/customers/customers.csv"));

        GZIPOutputStream gzipOutput = new GZIPOutputStream(new FileOutputStream("output_compressed/customers/customers.csv.gz"));

        OutputStreamWriter compressedWriter = new OutputStreamWriter(gzipOutput);

        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.EXCEL.withQuote('"').withQuoteMode(QuoteMode.NON_NUMERIC)
                    .withHeader("CustomerId", "Title", "First", "Last", "Gender", "JoinDate"));

        CSVPrinter compressedCSVPrinter = new CSVPrinter(compressedWriter, CSVFormat.EXCEL.withQuote('"').withQuoteMode(QuoteMode.NON_NUMERIC)
                .withHeader("CustomerId", "Title", "First", "Last", "Gender", "JoinDate"));

        String parquetPath = "output_parquet/customers/customers.snappy.parquet";

        ParquetWriter<Customer> parquetWriter = AvroParquetWriter.<Customer>builder(new Path(parquetPath))
                .withSchema(Customer.SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withRowGroupSize(rowGroupSize)
                .build();

        for (Customer customer: customers)
        {
            csvPrinter.printRecord(customer.getCustomerId(), customer.getTitle(),
                    customer.getFirstName(), customer.getLastName(), customer.getGenderString(),
                    dateFormat.format(customer.getJoinDate()));
            compressedCSVPrinter.printRecord(customer.getCustomerId(), customer.getTitle(),
                    customer.getFirstName(), customer.getLastName(), customer.getGenderString(),
                    dateFormat.format(customer.getJoinDate()));
            parquetWriter.write(customer);
        }

        csvPrinter.flush();
        writer.close();

        compressedCSVPrinter.flush();
        compressedWriter.close();
        gzipOutput.close();

        parquetWriter.close();
    }


    public String getTitle(boolean male)
    {
        if (male)
        {
            return "Mr";
        } else
        {
            switch (dataGenerator.nextInt(0, 3))
            {
                case 0:
                    return "Mrs";
                case 1:
                    return "Ms";
                case 2:
                    return "Miss";
                default:
                    return "Mrs";
            }
        }
    }

    public static void main(String [] args) throws ParseException, IOException
    {
        DataGenerator generator = new DataGenerator();

        List<Customer> customers = generator.createCustomers(1000000);
        generator.saveCustomers(customers);
        generator.createContributions(customers);

    }

}
