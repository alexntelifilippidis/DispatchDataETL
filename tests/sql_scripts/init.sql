CREATE DATABASE IF NOT EXISTS dbo;
USE dbo;

CREATE TABLE IF NOT EXISTS Packages (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    IssueDate DATETIME NOT NULL,
    Barcode NVARCHAR(30) NOT NULL,
    WeightKg DECIMAL(6, 2) NOT NULL,
    LengthCm DECIMAL(6, 2) NOT NULL,
    WidthCm DECIMAL(6, 2) NOT NULL,
    HeightCm DECIMAL(6, 2) NOT NULL,
    VolumeWeight DECIMAL(6, 2) NOT NULL,
    ImageFile NVARCHAR(150) NOT NULL
);

INSERT IGNORE INTO Packages (IssueDate, Barcode, WeightKg, LengthCm, WidthCm, HeightCm, VolumeWeight, ImageFile)
VALUES
    ('2024-04-08 12:00:00', 'ABC123', 2.5, 10.5, 5.5, 8.0, 3.0, 'image1.jpg'),
    ('2024-04-09 13:00:00', 'DEF456', 3.2, 12.0, 6.0, 9.0, 3.5, 'image2.jpg');
