package com.aws.datagen.model;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.Date;

public class Contribution extends SpecificRecordBase implements SpecificRecord
{
    private long contributionId = 0L;
    private long customerId = 0L;

    private double amount = 0.0;
    private Date contributionDate = null;

    private static final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd");

    public static final Schema SCHEMA = makeSchema();

    private static Schema makeSchema()
    {
        Schema schema = SchemaBuilder
                .record("Contribution").namespace("com.aws.parquet")
                .fields()
                .name("ContributionId").type().longType().longDefault(0L)
                .name("CustomerId").type().longType().longDefault(0L)
                .name("Amount").type().doubleType().doubleDefault(0.0)
                .name("ContributionDate").type().stringType().noDefault()
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
                return contributionId;
            }
            case 1:
            {
                return customerId;
            }
            case 2:
            {
                return amount;
            }
            case 3:
            {
                return dateFormat.format(contributionDate);
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
                contributionId = (Long) value;
                return;
            }
            case 1:
            {
                customerId = (Long) value;
                return;
            }
            case 2:
            {
                amount = (Double) value;
                return;
            }
            case 3:
            {
                try
                {
                    contributionDate = dateFormat.parse((String) value);
                }
                catch (ParseException e)
                {
                    contributionDate = null;
                    e.printStackTrace();
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

    public Contribution(long contributionId, long customerId)
    {
        this.contributionId = contributionId;
        this.customerId = customerId;
    }

    public long getContributionId()
    {
        return contributionId;
    }

    public long getCustomerId()
    {
        return customerId;
    }

    public double getAmount()
    {
        return amount;
    }

    public void setAmount(double amount)
    {
        this.amount = amount;
    }

    public Date getContributionDate()
    {
        return contributionDate;
    }

    public void setContributionDate(Date contributionDate)
    {
        this.contributionDate = contributionDate;
    }

    public String toString()
    {
        return String.format("%d\t%d\t%s\t%.2f",
                contributionId,
                customerId,
                contributionDate != null ? dateFormat.format(contributionDate) : "null",
                amount);
    }
}
