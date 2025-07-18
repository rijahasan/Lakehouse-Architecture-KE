CREATE TABLE `Ageing_NEW` (
    `12MonthsAmount` DOUBLE NOT NULL,
    `12MonthsAvgAmount` DOUBLE NOT NULL,
    `12MonthsAvgGrossBilled` DOUBLE NOT NULL,
    `12MonthsAvgNetCredit` DOUBLE NOT NULL,
    `12MonthsAvgUnits` DOUBLE NOT NULL,
    `12MonthsGrossBilled` DOUBLE NOT NULL,
    `12MonthsNetCredit` DOUBLE NOT NULL,
    `12MonthsUnits` BIGINT NOT NULL,
    `AccountContract` CHAR(255) NOT NULL,
    `AdjustedAmount` DOUBLE NOT NULL,
    `AdjustedUnits` BIGINT NOT NULL,
    `Adjustment` DOUBLE NOT NULL,
    `AmountBilled` DOUBLE NOT NULL,
    `AssessedAmount` DOUBLE NOT NULL,
    `AssessedUnits` BIGINT NOT NULL,
    `AverageAmount` DOUBLE NOT NULL,
    `AverageUnits` BIGINT NOT NULL,
    `BankCharges` DOUBLE NOT NULL,
    `BCM` VARCHAR(255) NOT NULL,
    `Billing_Class` VARCHAR(255) NOT NULL,
    `BillingMonth` DATE NOT NULL,
    `BillType` VARCHAR(255) NOT NULL,
    `Business_Partner` VARCHAR(255) NOT NULL,
    `CACreationDate` DATE NOT NULL,
    `ClearingAmount` DOUBLE NOT NULL,
    `ClosingBalance` DOUBLE NOT NULL,
    `ConnectedLoad` BIGINT NOT NULL,
    `ConsumerNumber` VARCHAR(255) NOT NULL,
    `ConsumerType` VARCHAR(255) NOT NULL,
    `ConsumerCounter` BIGINT NOT NULL,
    `Contract` VARCHAR(255) NOT NULL,
    `ContractAccount` VARCHAR(255) NOT NULL,
    `ContractCreationDate` DATE NOT NULL,
    `CurrentAmount` DOUBLE NOT NULL,
    `CurrentUnits` BIGINT NOT NULL,
    `CycleDay` BIGINT NOT NULL,
    `DeviceCategory` VARCHAR(255) NOT NULL,
    `DownPayment` DOUBLE NOT NULL,
    `DownPaymentRequest` DOUBLE NOT NULL,
    `GrossBilledYTD` DOUBLE NOT NULL,
    `IBC` VARCHAR(255) NOT NULL,
    `IBCName` VARCHAR(255) NOT NULL,
    `IndustryClassification` VARCHAR(255) NOT NULL,
    `InstallementAmount` DOUBLE NOT NULL,
    `InstallementNumber` VARCHAR(255) NOT NULL,
    `IRBDetectionAmount` DOUBLE NOT NULL,
    `IRBDetectionUnits` BIGINT NOT NULL,
    `IRBRevisedAmount` DOUBLE NOT NULL,
    `IRBRevisedUnits` BIGINT NOT NULL,
    `LastPaymentAmount` DOUBLE NOT NULL,
    `LastPaymentDate` DATE NOT NULL,
    `LPSBilled` DOUBLE NOT NULL,
    `LPSWaived` DOUBLE NOT NULL,
    `MeterMake` VARCHAR(255) NOT NULL,
    `MeterNumber` VARCHAR(255) NOT NULL,
    `MRU` VARCHAR(255) NOT NULL,
    `NetCreditYTD` DOUBLE NOT NULL,
    `NormalAmount` DOUBLE NOT NULL,
    `NormalUnits` BIGINT NOT NULL,
    `OIP` VARCHAR(255) NOT NULL,
    `Payment` DOUBLE NOT NULL,
    `PMT` VARCHAR(255) NOT NULL,
    `PremiseType` VARCHAR(255) NOT NULL,
    `PreviousYearAllowance` DOUBLE NOT NULL,
    `PSCClassification` VARCHAR(255) NOT NULL,
    `PSCConsumerIBC` VARCHAR(255) NOT NULL,
    `PSCConsumerRegion` VARCHAR(255) NOT NULL,
    `PSCDepartment` VARCHAR(255) NOT NULL,
    `PSCLocation` VARCHAR(255) NOT NULL,
    `PSCMinistry` VARCHAR(255) NOT NULL,
    `RateCategory` VARCHAR(255) NOT NULL,
    `Region` VARCHAR(255) NOT NULL,
    `RegularAmount` DOUBLE NOT NULL,
    `RegularUnits` BIGINT NOT NULL,
    `SanctionedLoad` BIGINT NOT NULL,
    `Status` VARCHAR(255) NOT NULL,
    `Strategic_NonStrategic` VARCHAR(255) NOT NULL,
    `UnitBilledYTD` BIGINT NOT NULL,
    `WriteOff` BIGINT NOT NULL,
    `PSCSubDepartment` VARCHAR(255) NOT NULL,
    PRIMARY KEY (`AccountContract`) -- Assuming AccountContract as a unique identifier instead of FLOAT
);
