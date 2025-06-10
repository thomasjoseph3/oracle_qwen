import oracledb
from dotenv import load_dotenv
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables from .env file
load_dotenv()

# Retrieve database connection details from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_SERVICE_NAME = os.getenv("DB_SERVICE_NAME")
DB_PRIVILEGE = os.getenv("DB_PRIVILEGE", "").upper()  # Default to normal mode if not specified

# Validate that all required environment variables are set
required_vars = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_SERVICE_NAME"]
missing_vars = [var for var in required_vars if os.getenv(var) is None]

if missing_vars:
    missing = ", ".join(missing_vars)
    logging.error(f"Missing required environment variables: {missing}")
    raise EnvironmentError(f"Missing required environment variables: {missing}")

# Create DSN (Data Source Name) - simplified for oracledb
dsn = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}"

# Test connection first
try:
    logging.info(f"Attempting to connect to Oracle DB at {dsn}")
    
    # Handle privilege mode for oracledb
    if DB_PRIVILEGE == "SYSDBA":
        connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn, mode=oracledb.AUTH_MODE_SYSDBA)
    elif DB_PRIVILEGE == "SYSOPER":
        connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn, mode=oracledb.AUTH_MODE_SYSOPER)
    else:
        connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn)
    
    cursor = connection.cursor()
    logging.info("Database connection established successfully.")
    
    # Test basic query
    cursor.execute("SELECT 1 FROM DUAL")
    result = cursor.fetchone()
    logging.info(f"Connection test successful: {result}")
    
except oracledb.DatabaseError as e:
    logging.error(f"Failed to connect to the database: {str(e)}")
    logging.error("Please check:")
    logging.error("1. Oracle container is running")
    logging.error("2. Port 1521 is exposed and accessible")
    logging.error("3. Service name 'freepdb1' exists")
    logging.error("4. User credentials are correct")
    raise SystemExit(1)

# Drop existing tables (in correct order to handle foreign key constraints)
drop_table_queries = [
    "DROP TABLE wheels_temperature CASCADE CONSTRAINTS",
    "DROP TABLE wheels_load CASCADE CONSTRAINTS", 
    "DROP TABLE work_orders CASCADE CONSTRAINTS",
    "DROP TABLE assets_maintenance CASCADE CONSTRAINTS",
    "DROP TABLE lifts CASCADE CONSTRAINTS",
    "DROP TABLE trolleys CASCADE CONSTRAINTS",
    "DROP TABLE inventory CASCADE CONSTRAINTS",
    "DROP TABLE vessels CASCADE CONSTRAINTS",
    "DROP TABLE cradles CASCADE CONSTRAINTS",
    "DROP TABLE rails CASCADE CONSTRAINTS",
    "DROP TABLE assets CASCADE CONSTRAINTS",
]

logging.info("Dropping existing tables...")
for query in drop_table_queries:
    try:
        cursor.execute(query)
        table_name = query.split()[2]
        logging.info(f"Table dropped successfully: {table_name}")
    except oracledb.DatabaseError as e:
        if "ORA-00942" in str(e):  # Table or view does not exist
            table_name = query.split()[2]
            logging.info(f"Table {table_name} doesn't exist, skipping...")
        else:
            table_name = query.split()[2]
            logging.warning(f"Error dropping table {table_name}: {str(e)}")

# Create tables (in correct order for foreign key dependencies)
create_table_queries = [
    """
    CREATE TABLE assets (
        id VARCHAR2(100) PRIMARY KEY,
        assetType VARCHAR2(100),
        name VARCHAR2(100),
        description VARCHAR2(255),
        status VARCHAR2(50),
        createdAt TIMESTAMP,
        updatedAt TIMESTAMP
    )
    """,
    """
    CREATE TABLE cradles (
        id VARCHAR2(100) PRIMARY KEY,
        updatedAt TIMESTAMP,
        cradleName VARCHAR2(100),
        capacity NUMBER,
        maxShipLength NUMBER,
        status VARCHAR2(50),
        location VARCHAR2(100),
        lastMaintenanceDate TIMESTAMP,
        nextMaintenanceDue TIMESTAMP,
        operationalSince TIMESTAMP,
        notes VARCHAR2(255),
        occupancy VARCHAR2(100),
        currentLoad NUMBER,
        structuralStress VARCHAR2(50),
        wearLevel VARCHAR2(50),
        assetId VARCHAR2(100),
        CONSTRAINT fkCradleAssetId FOREIGN KEY (assetId) REFERENCES assets(id)
    )
    """,
    """
    CREATE TABLE vessels (
        id VARCHAR2(100) PRIMARY KEY,
        updatedAt TIMESTAMP,
        vesselName VARCHAR2(100) UNIQUE,
        vesselType VARCHAR2(50),
        weight NUMBER,
        length NUMBER,
        width NUMBER,
        draft NUMBER,
        status VARCHAR2(50),
        lastMaintenanceDate TIMESTAMP,
        nextMaintenanceDue TIMESTAMP,
        birthingArea VARCHAR2(100),
        operationalSince TIMESTAMP,
        ownerCompany VARCHAR2(100),
        notes VARCHAR2(255),
        assignedCradle VARCHAR2(100),
        transferCompleted VARCHAR2(50),
        estimatedTimeToDestination VARCHAR2(50),
        bearingTemperature NUMBER,
        assetId VARCHAR2(100),
        CONSTRAINT fkVesselAssetId FOREIGN KEY (assetId) REFERENCES assets(id),
        CONSTRAINT fkVesselAssignedCradle FOREIGN KEY (assignedCradle) REFERENCES cradles(id)
    )
    """,
    """
    CREATE TABLE rails (
        id VARCHAR2(100) PRIMARY KEY,
        updatedAt TIMESTAMP,
        railName VARCHAR2(100),
        length NUMBER,
        capacity NUMBER,
        status VARCHAR2(50),
        lastInspectionDate TIMESTAMP,
        nextInspectionDue TIMESTAMP,
        operationalSince TIMESTAMP,
        notes VARCHAR2(255),
        assetId VARCHAR2(100),
        CONSTRAINT fkRailAssetId FOREIGN KEY (assetId) REFERENCES assets(id)
    )
    """,
    """
    CREATE TABLE trolleys (
        id VARCHAR2(100) PRIMARY KEY,
        updatedAt TIMESTAMP,
        trolleyName VARCHAR2(100),
        wheelCount NUMBER,
        railId VARCHAR2(100),
        assignedVesselId VARCHAR2(100),
        status VARCHAR2(50),
        lastMaintenanceDate TIMESTAMP,
        nextMaintenanceDue TIMESTAMP,
        notes VARCHAR2(255),
        maxCapacity NUMBER,
        currentLoad NUMBER,
        speed NUMBER,
        location VARCHAR2(255),
        utilizationRate VARCHAR2(50),
        averageTransferTime VARCHAR2(50),
        assetId VARCHAR2(100),
        CONSTRAINT fkTrolleyAssetId FOREIGN KEY (assetId) REFERENCES assets(id),
        CONSTRAINT fkTrolleyRailId FOREIGN KEY (railId) REFERENCES rails(id),
        CONSTRAINT fkTrolleyAssignedVesselId FOREIGN KEY (assignedVesselId) REFERENCES vessels(id)
    )
    """,
    """
    CREATE TABLE lifts (
        id VARCHAR2(100) PRIMARY KEY,
        updatedAt TIMESTAMP,
        liftName VARCHAR2(100),
        platformLength NUMBER,
        platformWidth NUMBER,
        maxShipDraft NUMBER,
        location VARCHAR2(255),
        status VARCHAR2(50),
        lastMaintenanceDate TIMESTAMP,
        nextMaintenanceDue TIMESTAMP,
        operationalSince TIMESTAMP,
        assignedVesselId VARCHAR2(100),
        notes VARCHAR2(255),
        currentLoad NUMBER,
        historicalUsageHours NUMBER,
        maxCapacity NUMBER,
        utilizationRate VARCHAR2(50),
        averageTransferTime VARCHAR2(50),
        assetId VARCHAR2(100),
        CONSTRAINT fkLiftAssetId FOREIGN KEY (assetId) REFERENCES assets(id),
        CONSTRAINT fkLiftAssignedVesselId FOREIGN KEY (assignedVesselId) REFERENCES vessels(id)
    )
    """,
    """
    CREATE TABLE inventory (
        id VARCHAR2(100) PRIMARY KEY,
        updatedAt TIMESTAMP,
        lastUpdated TIMESTAMP,
        name VARCHAR2(100),
        location VARCHAR2(100),
        quantity NUMBER,
        assetId VARCHAR2(100),
        CONSTRAINT fkInventoryAssetId FOREIGN KEY (assetId) REFERENCES assets(id)
    )
    """,
    """
    CREATE TABLE assets_maintenance (
        id VARCHAR2(100) PRIMARY KEY,
        updatedAt TIMESTAMP,
        assetId VARCHAR2(100),
        description VARCHAR2(255),
        datePerformed TIMESTAMP,
        performedBy VARCHAR2(255),
        nextDueDate TIMESTAMP,
        assetName VARCHAR2(100),
        historicalUsageHours NUMBER,
        remainingLifespanHours NUMBER,
        statusSummary VARCHAR2(255),
        shipsInTransfer NUMBER,
        operationalLifts NUMBER,
        operationalTrolleys NUMBER,
        CONSTRAINT fkMaintenanceAssetId FOREIGN KEY (assetId) REFERENCES assets(id)
    )
    """,
    """
    CREATE TABLE work_orders (
        id VARCHAR2(100) PRIMARY KEY,
        updatedAt TIMESTAMP,
        workType VARCHAR2(50),
        assignedTo VARCHAR2(100),
        startDate TIMESTAMP,
        endDate TIMESTAMP,
        status VARCHAR2(50),
        notes VARCHAR2(255),
        vesselName VARCHAR2(100),
        vesselId VARCHAR2(100), 
        CONSTRAINT fkWorkOrderVesselId FOREIGN KEY (vesselId) REFERENCES vessels(id)   
    )
    """,
    """
    CREATE TABLE wheels_load (
        id VARCHAR2(100) PRIMARY KEY,
        updatedAt TIMESTAMP,
        trolley VARCHAR2(100),
        wheel VARCHAR2(100),
        currentLoad NUMBER,
        CONSTRAINT fkWheelsLoadTrolleyId FOREIGN KEY (trolley) REFERENCES trolleys(id)
    )
    """,
    """
    CREATE TABLE wheels_temperature (
        id VARCHAR2(100) PRIMARY KEY,
        updatedAt TIMESTAMP,
        trolley VARCHAR2(100),
        wheel VARCHAR2(100),
        bearingTemperature NUMBER,
        CONSTRAINT fkWheelsTempTrolleyId FOREIGN KEY (trolley) REFERENCES trolleys(id)
    )
    """
]

# Execute table creation queries
logging.info("Creating tables...")
tables_created = 0
for query in create_table_queries:
    try:
        cursor.execute(query)
        table_name = query.strip().split()[2]
        logging.info(f"Table created successfully: {table_name}")
        tables_created += 1
    except oracledb.DatabaseError as e:
        table_name = query.strip().split()[2]
        logging.error(f"Error creating table {table_name}: {str(e)}")

# Commit changes
connection.commit()
logging.info(f"Database setup completed successfully! {tables_created} tables created.")

# Verify tables were created
logging.info("Verifying table creation...")
cursor.execute("""
    SELECT table_name 
    FROM user_tables 
    ORDER BY table_name
""")
tables = cursor.fetchall()
logging.info(f"Tables in database: {[table[0] for table in tables]}")

# Close connection
cursor.close()
connection.close()
logging.info("Database connection closed.")