Feature: Banking Transaction Data Quality
    As a Data Quality Engineeer
    I want to validate incoming transaction Data
    so that downstream systems receive clean, reliable Data

    Background:
        Given the raw transactions table is loaded with recent Data

    @completeness
    Scenario: All mandatory fields are populated
        When I check for null values in mandatory fields
        Then the following fields should have zero nulls:
          | fields                |
          | transaction_id        |
          | account_id            |
          | transaction_timestamp |

    @accuracy
    Scenario: "Only valid currencies are present
        When I check the currency field
        Then all values should be one of:
           | currency   |
           | GBP        |
           | EUR        |
           | USD        |
           | CHF        |

    @consistency
    Scenario: Minimum daily transaction volume is met
        When I count all transactions
        Then there should be at least 100 transactions