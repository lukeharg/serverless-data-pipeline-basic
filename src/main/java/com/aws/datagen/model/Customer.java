package com.aws.datagen.model;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Customer extends SpecificRecordBase implements SpecificRecord
{
    private long customerId = 0L;
    private String title = null;
    private String firstName = null;
    private String lastName = null;
    private boolean male = true;
    private Date joinDate = null;
    private double salaryVariance = 0.0;

    private static final Random random = new Random();

    private Map<Integer, Double> yearlyVariance = new HashMap<>();

    private static final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd");

    public static final Schema SCHEMA = makeSchema();

    private static Schema makeSchema()
    {
        Schema schema = SchemaBuilder
                .record("Customer").namespace("com.aws.parquet")
                .fields()
                .name("CustomerId").type().longType().noDefault()
                .name("Title").type().stringType().noDefault()
                .name("FirstName").type().stringType().noDefault()
                .name("LastName").type().stringType().noDefault()
                .name("Gender").type().stringType().noDefault()
                .name("JoinDate").type().stringType().noDefault()
                .endRecord();

        return schema;
    }

    @Override
    public Object get(int field)
    {
        switch (field)
        {
            case 0:
            {
                return customerId;
            }
            case 1:
            {
                return title;
            }
            case 2:
            {
                return firstName;
            }
            case 3:
            {
                return lastName;
            }
            case 4:
            {
                return getGenderString();
            }
            case 5:
            {
                return dateFormat.format(joinDate);
            }
            default:
            {
                throw new IllegalArgumentException("Invalid field index: " + field);
            }
        }
    }

    @Override
    public void put(int field, Object value)
    {
        switch (field)
        {
            case 0:
            {
                customerId = (Long) value;
                return;
            }
            case 1:
            {
                title = (String) value;
                return;
            }
            case 2:
            {
                firstName = (String) value;
                return;
            }
            case 3:
            {
                lastName = (String) value;
                return;
            }
            case 4:
            {
                male = "M".equals(value);
                return;
            }
            case 5:
            {
                try
                {
                    joinDate = dateFormat.parse((String) value);
                }
                catch (ParseException e)
                {
                    e.printStackTrace();
                    joinDate = null;
                }
                return;
            }
            default:
            {
                throw new IllegalArgumentException("Invalid field index: " + field);
            }
        }
    }

    @Override
    public Schema getSchema()
    {
        return SCHEMA;
    }

    public Customer(long customerId)
    {
        this.customerId = customerId;
    }

    /**
     * Fetches the yearly salary for this customer
     */
    public double getSalary(int minYear, int currentYear, double averageSalary, double growthFactor)
    {
        if (!yearlyVariance.containsKey(currentYear))
        {
            double kick = 0.2 * random.nextDouble();

            if (random.nextBoolean())
            {
                kick *= -1.0;
            }

            yearlyVariance.put(currentYear, salaryVariance + kick);
        }

        double actualVariance = yearlyVariance.get(currentYear);

        int yearsOfGrowth = currentYear - minYear;

        double salaryFirstYear = averageSalary + averageSalary * actualVariance;

        double yearlyGrowth = salaryFirstYear * growthFactor;

        double salary = salaryFirstYear + yearlyGrowth * yearsOfGrowth;

        return salary;
    }

    public long getCustomerId()
    {
        return customerId;
    }

    public String getFirstName()
    {
        return firstName;
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle(String title)
    {
        this.title = title;
    }

    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    public String getLastName()
    {
        return lastName;
    }

    public void setLastName(String lastName)
    {
        this.lastName = lastName;
    }

    public Date getJoinDate()
    {
        return joinDate;
    }

    public void setJoinDate(Date joinDate)
    {
        this.joinDate = joinDate;
    }

    public void setSalaryVariance(double salaryVariance)
    {
        this.salaryVariance = salaryVariance;
    }

    public boolean getMale()
    {
        return male;
    }

    public void setMale(boolean male)
    {
        this.male = male;
    }

    public String getGenderString()
    {
        return male ? "M" : "F";
    }

    public String toString()
    {
        return String.format("%d\t%s %s %s\t%s\t%s",
                customerId,
                title, firstName, lastName,
                getGenderString(),
                joinDate != null ? dateFormat.format(joinDate) : "null");
    }
}
