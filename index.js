import 'dotenv/config';
import { Client, Databases, ID, Query } from 'node-appwrite';
import { confirm, select } from '@inquirer/prompts';
import cliProgress from 'cli-progress';
import { styleText } from 'node:util';
import { writeFileSync, readFileSync, unlinkSync, existsSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';

// JSON cache file path
const CACHE_FILE_PATH = join(process.cwd(), '.appwrite-clone-cache.json');

// CSV export directory
const CSV_EXPORT_DIR = join(process.cwd(), 'csv-export');

// Configuration
const config = {
    endpoint: process.env.APPWRITE_ENDPOINT,
    projectId: process.env.APPWRITE_PROJECT_ID,
    apiKey: process.env.APPWRITE_API_KEY,
    sourceDatabaseId: process.env.SOURCE_DATABASE_ID,
    destDatabaseId: process.env.DEST_DATABASE_ID,
    batchSize: parseInt(process.env.BATCH_SIZE) || 100,
    // Clone mode: 'full' (default), 'structure-only', 'data-only'
    cloneMode: process.env.CLONE_MODE || 'full',
};

// Validate configuration
function validateConfig() {
    const required = [
        'APPWRITE_ENDPOINT',
        'APPWRITE_PROJECT_ID',
        'APPWRITE_API_KEY',
        'SOURCE_DATABASE_ID',
        'DEST_DATABASE_ID',
    ];

    const missing = required.filter((key) => !process.env[key]);

    if (missing.length > 0) {
        console.error('Missing required environment variables:');
        missing.forEach((key) => console.error(`  - ${key}`));
        console.error('\nPlease copy .env.example to .env and fill in the values.');
        process.exit(1);
    }
}

// Initialize Appwrite client
function createClient() {
    const client = new Client();
    client
        .setEndpoint(config.endpoint)
        .setProject(config.projectId)
        .setKey(config.apiKey);

    return client;
}

// Write data to JSON cache file
function writeCacheFile(data) {
    console.log(`\n  Writing data to cache file: ${CACHE_FILE_PATH}`);
    writeFileSync(CACHE_FILE_PATH, JSON.stringify(data, null, 2), 'utf-8');
    console.log(`  Cache file created successfully`);
}

// Read data from JSON cache file
function readCacheFile() {
    console.log(`\n  Reading data from cache file: ${CACHE_FILE_PATH}`);
    const data = JSON.parse(readFileSync(CACHE_FILE_PATH, 'utf-8'));
    console.log(`  Cache file read successfully`);
    return data;
}

// Delete JSON cache file
function deleteCacheFile() {
    if (existsSync(CACHE_FILE_PATH)) {
        unlinkSync(CACHE_FILE_PATH);
        console.log(`\n  Cache file deleted: ${CACHE_FILE_PATH}`);
    }
}

// Escape CSV field value
function escapeCSVField(value) {
    if (value === null || value === undefined) {
        return '';
    }

    // Convert to string
    let str = typeof value === 'object' ? JSON.stringify(value) : String(value);

    // If contains comma, quote, newline, or starts/ends with space, wrap in quotes
    if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r') || str.startsWith(' ') || str.endsWith(' ')) {
        // Escape quotes by doubling them
        str = '"' + str.replace(/"/g, '""') + '"';
    }

    return str;
}

// Convert documents to CSV format
function documentsToCSV(documents, includeSystemFields = false) {
    if (documents.length === 0) {
        return '';
    }

    const systemFields = [
        '$id',
        '$collectionId',
        '$databaseId',
        '$createdAt',
        '$updatedAt',
        '$permissions',
        '$sequence',
    ];

    // Get all unique keys from all documents
    const allKeys = new Set();
    for (const doc of documents) {
        for (const key of Object.keys(doc)) {
            if (includeSystemFields || !systemFields.includes(key)) {
                allKeys.add(key);
            }
        }
    }

    // Sort keys: system fields first (if included), then alphabetically
    const headers = Array.from(allKeys).sort((a, b) => {
        const aIsSystem = a.startsWith('$');
        const bIsSystem = b.startsWith('$');
        if (aIsSystem && !bIsSystem) return -1;
        if (!aIsSystem && bIsSystem) return 1;
        return a.localeCompare(b);
    });

    // Build CSV content
    const lines = [];

    // Header row
    lines.push(headers.map(h => escapeCSVField(h)).join(','));

    // Data rows
    for (const doc of documents) {
        const row = headers.map(header => escapeCSVField(doc[header]));
        lines.push(row.join(','));
    }

    return lines.join('\n');
}

// Export collection to CSV file
function exportCollectionToCSV(collectionName, documents, includeSystemFields = false) {
    // Create export directory if it doesn't exist
    if (!existsSync(CSV_EXPORT_DIR)) {
        mkdirSync(CSV_EXPORT_DIR, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const filename = `${collectionName}_${timestamp}.csv`;
    const filepath = join(CSV_EXPORT_DIR, filename);

    const csvContent = documentsToCSV(documents, includeSystemFields);
    writeFileSync(filepath, csvContent, 'utf-8');

    return { filepath, filename, recordCount: documents.length };
}

// Export all collections to CSV files
async function exportToCSV(databases, sourceDbId, includeSystemFields = false) {
    console.log('\n' + styleText('cyan', '--- Exporting to CSV ---'));

    const results = {
        collections: [],
        totalRecords: 0,
        exportDir: CSV_EXPORT_DIR
    };

    // Fetch all collections from source
    console.log('\n  Fetching collections from source database...');
    const collections = await fetchAllCollections(databases, sourceDbId);

    if (collections.length === 0) {
        console.log('  No collections found in source database.');
        return results;
    }

    console.log(`  Found ${collections.length} collections to export.\n`);

    // Create export directory
    if (!existsSync(CSV_EXPORT_DIR)) {
        mkdirSync(CSV_EXPORT_DIR, { recursive: true });
    }
    console.log(`  Export directory: ${CSV_EXPORT_DIR}\n`);

    const progressBar = createProgressBar(
        '  Exporting |{bar}| {percentage}% | {value}/{total} | {collection}',
        collections.length
    );

    for (const collection of collections) {
        progressBar.update({ collection: collection.name.substring(0, 20).padEnd(20) });

        // Fetch all documents
        const documents = await fetchAllDocuments(databases, sourceDbId, collection.$id);

        if (documents.length > 0) {
            const exportResult = exportCollectionToCSV(collection.name, documents, includeSystemFields);
            results.collections.push({
                name: collection.name,
                ...exportResult
            });
            results.totalRecords += documents.length;
        } else {
            results.collections.push({
                name: collection.name,
                filepath: null,
                filename: null,
                recordCount: 0
            });
        }

        progressBar.increment();
    }

    progressBar.stop();

    return results;
}

// Create progress bar
function createProgressBar(format, total) {
    const bar = new cliProgress.SingleBar({
        format: format,
        barCompleteChar: '\u2588',
        barIncompleteChar: '\u2591',
        hideCursor: true,
        clearOnComplete: false,
        stopOnComplete: true,
    }, cliProgress.Presets.shades_classic);
    bar.start(total, 0);
    return bar;
}

// Get database info
async function getDatabaseInfo(databases, databaseId) {
    try {
        const db = await databases.get(databaseId);
        return { exists: true, name: db.name, id: db.$id };
    } catch (error) {
        return { exists: false, name: null, id: databaseId };
    }
}

// Drop all collections in destination database
async function dropDestinationCollections(databases, databaseId) {
    console.log('\nDropping existing collections in destination database...');

    const collections = await fetchAllCollections(databases, databaseId);

    if (collections.length === 0) {
        console.log('  No collections to drop.');
        return { dropped: 0, errors: [] };
    }

    const progressBar = createProgressBar(
        '  Dropping |{bar}| {percentage}% | {value}/{total} collections',
        collections.length
    );

    let dropped = 0;
    const errors = [];

    for (const collection of collections) {
        try {
            await databases.deleteCollection(databaseId, collection.$id);
            dropped++;
        } catch (error) {
            errors.push({ collection: collection.name, error: error.message });
        }
        progressBar.increment();
    }

    progressBar.stop();
    console.log(`  Dropped ${dropped} collections, ${errors.length} errors`);

    return { dropped, errors };
}

// Confirm migration with user
async function confirmMigration(sourceDb, destDb) {
    console.log('\n' + '='.repeat(60));
    console.log(styleText('bold', '⚠️  DATABASE MIGRATION CONFIRMATION'));
    console.log('='.repeat(60));

    console.log('\n' + styleText('cyan', 'Source Database (FROM):'));
    console.log(`  ID:   ${styleText('yellow', sourceDb.id)}`);
    console.log(`  Name: ${sourceDb.exists ? styleText('green', sourceDb.name) : styleText('red', 'NOT FOUND')}`);

    console.log('\n' + styleText('cyan', 'Destination Database (TO):'));
    console.log(`  ID:   ${styleText('yellow', destDb.id)}`);
    console.log(`  Name: ${destDb.exists ? styleText('green', destDb.name) : styleText('red', 'NOT FOUND')}`);

    console.log('\n' + styleText('red', styleText('bold', 'WARNING:')));
    console.log(styleText('red', '  - ALL existing collections in the destination will be DELETED'));
    console.log(styleText('red', '  - This action CANNOT be undone'));
    console.log('');

    // First confirmation
    const confirmSource = await confirm({
        message: `Is "${sourceDb.id}" the correct SOURCE database?`,
        default: false,
    });

    if (!confirmSource) {
        console.log('\nMigration cancelled by user.');
        process.exit(0);
    }

    // Second confirmation
    const confirmDest = await confirm({
        message: `Is "${destDb.id}" the correct DESTINATION database to overwrite?`,
        default: false,
    });

    if (!confirmDest) {
        console.log('\nMigration cancelled by user.');
        process.exit(0);
    }

    // Final confirmation
    const finalConfirm = await confirm({
        message: styleText('red', 'Are you ABSOLUTELY SURE you want to proceed? This will DELETE all data in the destination.'),
        default: false,
    });

    if (!finalConfirm) {
        console.log('\nMigration cancelled by user.');
        process.exit(0);
    }

    return true;
}

// Select clone mode interactively
async function selectCloneMode() {
    const mode = await select({
        message: 'Select clone mode:',
        choices: [
            {
                name: 'Full Clone (structure + data)',
                value: 'full',
                description: 'Clone collections, attributes, indexes, and all documents',
            },
            {
                name: 'Structure Only',
                value: 'structure-only',
                description: 'Clone only collections, attributes, and indexes (no documents)',
            },
            {
                name: 'Data Only',
                value: 'data-only',
                description: 'Clone only documents (destination collections must exist)',
            },
            {
                name: 'Add Missing Records Only',
                value: 'missing-only',
                description: 'Only add documents that do not exist in destination (incremental sync)',
            },
            {
                name: 'Export to CSV',
                value: 'export-csv',
                description: 'Export all source documents to CSV files (one file per collection)',
            },
        ],
    });

    return mode;
}

// Fetch all attributes from a collection
async function fetchAllAttributes(databases, databaseId, collectionId) {
    const attributes = [];
    let lastId = null;
    let hasMore = true;

    while (hasMore) {
        const queries = [Query.limit(100)];

        if (lastId) {
            queries.push(Query.cursorAfter(lastId));
        }

        const response = await databases.listAttributes(databaseId, collectionId, queries);
        attributes.push(...response.attributes);

        if (response.attributes.length < 100) {
            hasMore = false;
        } else {
            lastId = response.attributes[response.attributes.length - 1].key;
        }
    }

    return attributes;
}

// Fetch all indexes from a collection
async function fetchAllIndexes(databases, databaseId, collectionId) {
    const indexes = [];
    let lastId = null;
    let hasMore = true;

    while (hasMore) {
        const queries = [Query.limit(100)];

        if (lastId) {
            queries.push(Query.cursorAfter(lastId));
        }

        const response = await databases.listIndexes(databaseId, collectionId, queries);
        indexes.push(...response.indexes);

        if (response.indexes.length < 100) {
            hasMore = false;
        } else {
            lastId = response.indexes[response.indexes.length - 1].key;
        }
    }

    return indexes;
}

// Wait for attribute to be available (not processing)
async function waitForAttribute(databases, databaseId, collectionId, attributeKey, maxRetries = 30) {
    for (let i = 0; i < maxRetries; i++) {
        const attributes = await fetchAllAttributes(databases, databaseId, collectionId);
        const attribute = attributes.find(a => a.key === attributeKey);

        if (attribute && attribute.status === 'available') {
            return true;
        }

        // Wait 1 second before retrying
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    return false;
}

// Create an attribute in the destination collection
async function createAttribute(databases, databaseId, collectionId, attribute) {
    const { key, type, size, required, array, xdefault } = attribute;

    try {
        switch (type) {
            case 'string':
                await databases.createStringAttribute(
                    databaseId,
                    collectionId,
                    key,
                    size || 255,
                    required || false,
                    xdefault,
                    array || false
                );
                break;

            case 'integer':
                await databases.createIntegerAttribute(
                    databaseId,
                    collectionId,
                    key,
                    required || false,
                    attribute.min,
                    attribute.max,
                    xdefault,
                    array || false
                );
                break;

            case 'float':
            case 'double':
                await databases.createFloatAttribute(
                    databaseId,
                    collectionId,
                    key,
                    required || false,
                    attribute.min,
                    attribute.max,
                    xdefault,
                    array || false
                );
                break;

            case 'boolean':
                await databases.createBooleanAttribute(
                    databaseId,
                    collectionId,
                    key,
                    required || false,
                    xdefault,
                    array || false
                );
                break;

            case 'datetime':
                await databases.createDatetimeAttribute(
                    databaseId,
                    collectionId,
                    key,
                    required || false,
                    xdefault,
                    array || false
                );
                break;

            case 'email':
                await databases.createEmailAttribute(
                    databaseId,
                    collectionId,
                    key,
                    required || false,
                    xdefault,
                    array || false
                );
                break;

            case 'ip':
                await databases.createIpAttribute(
                    databaseId,
                    collectionId,
                    key,
                    required || false,
                    xdefault,
                    array || false
                );
                break;

            case 'url':
                await databases.createUrlAttribute(
                    databaseId,
                    collectionId,
                    key,
                    required || false,
                    xdefault,
                    array || false
                );
                break;

            case 'enum':
                await databases.createEnumAttribute(
                    databaseId,
                    collectionId,
                    key,
                    attribute.elements || [],
                    required || false,
                    xdefault,
                    array || false
                );
                break;

            case 'relationship':
                await databases.createRelationshipAttribute(
                    databaseId,
                    collectionId,
                    attribute.relatedCollection,
                    attribute.relationType,
                    attribute.twoWay || false,
                    key,
                    attribute.twoWayKey,
                    attribute.onDelete || 'restrict'
                );
                break;

            default:
                console.log(`      Warning: Unknown attribute type "${type}" for key "${key}", skipping...`);
                return { success: false, error: `Unknown type: ${type}` };
        }

        return { success: true };
    } catch (error) {
        return { success: false, error: error.message };
    }
}

// Create all attributes in the destination collection
async function createAttributes(databases, databaseId, collectionId, attributes, options = {}) {
    const { silent = false } = options;
    const log = silent ? () => {} : console.log;

    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    for (const attribute of attributes) {
        // Skip relationship attributes that were created by the two-way relationship
        if (attribute.type === 'relationship' && attribute.side === 'child') {
            log(`      Skipping child relationship attribute: ${attribute.key}`);
            continue;
        }

        const result = await createAttribute(databases, databaseId, collectionId, attribute);

        if (result.success) {
            successCount++;
            // Wait for attribute to be available before creating the next one
            await waitForAttribute(databases, databaseId, collectionId, attribute.key);
        } else {
            errorCount++;
            errors.push({ key: attribute.key, error: result.error });
        }
    }

    return { successCount, errorCount, errors };
}

// Create an index in the destination collection
async function createIndex(databases, databaseId, collectionId, index) {
    try {
        await databases.createIndex(
            databaseId,
            collectionId,
            index.key,
            index.type,
            index.attributes,
            index.orders || []
        );
        return { success: true };
    } catch (error) {
        return { success: false, error: error.message };
    }
}

// Wait for index to be available
async function waitForIndex(databases, databaseId, collectionId, indexKey, maxRetries = 60) {
    for (let i = 0; i < maxRetries; i++) {
        const indexes = await fetchAllIndexes(databases, databaseId, collectionId);
        const index = indexes.find(idx => idx.key === indexKey);

        if (index && index.status === 'available') {
            return true;
        }

        // Wait 1 second before retrying
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    return false;
}

// Create all indexes in the destination collection
async function createIndexes(databases, databaseId, collectionId, indexes) {
    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    for (const index of indexes) {
        const result = await createIndex(databases, databaseId, collectionId, index);

        if (result.success) {
            successCount++;
            // Wait for index to be available before creating the next one
            await waitForIndex(databases, databaseId, collectionId, index.key);
        } else {
            errorCount++;
            errors.push({ key: index.key, error: result.error });
        }
    }

    return { successCount, errorCount, errors };
}

// Clone a collection structure (create collection with attributes and indexes)
async function cloneCollectionStructure(databases, sourceDbId, destDbId, collection, options = {}) {
    const { silent = false } = options;
    const { $id, name, $permissions, documentSecurity, enabled } = collection;
    const log = silent ? () => {} : console.log;

    log(`\n  Cloning collection structure: ${name} (${$id})`);

    let attrResult = { successCount: 0, errorCount: 0, errors: [] };
    let idxResult = { successCount: 0, errorCount: 0, errors: [] };

    try {
        // Create the collection in destination database
        await databases.createCollection(
            destDbId,
            $id, // Use the same collection ID
            name,
            $permissions,
            documentSecurity,
            enabled
        );
        log(`    Collection created successfully`);

        // Fetch and create attributes
        log(`    Fetching attributes from source...`);
        const attributes = await fetchAllAttributes(databases, sourceDbId, $id);

        if (attributes.length > 0) {
            log(`    Creating ${attributes.length} attributes...`);
            attrResult = await createAttributes(databases, destDbId, $id, attributes, { silent });
            log(`    Attributes: ${attrResult.successCount} created, ${attrResult.errorCount} failed`);

            if (attrResult.errors.length > 0) {
                for (const err of attrResult.errors) {
                    log(`      - ${err.key}: ${err.error}`);
                }
            }
        } else {
            log(`    No attributes to create`);
        }

        // Fetch and create indexes
        log(`    Fetching indexes from source...`);
        const indexes = await fetchAllIndexes(databases, sourceDbId, $id);

        if (indexes.length > 0) {
            log(`    Creating ${indexes.length} indexes...`);
            idxResult = await createIndexes(databases, destDbId, $id, indexes);
            log(`    Indexes: ${idxResult.successCount} created, ${idxResult.errorCount} failed`);

            if (idxResult.errors.length > 0) {
                for (const err of idxResult.errors) {
                    log(`      - ${err.key}: ${err.error}`);
                }
            }
        } else {
            log(`    No indexes to create`);
        }

        // Return failure if any attributes failed - this is critical for data integrity
        if (attrResult.errorCount > 0) {
            return {
                success: false,
                error: `${attrResult.errorCount} attribute(s) failed to create`,
                attributeErrors: attrResult.errors,
                indexErrors: idxResult.errors
            };
        }

        return { success: true, attributeErrors: [], indexErrors: idxResult.errors };
    } catch (error) {
        log(`    Failed to create collection: ${error.message}`);
        return { success: false, error: error.message, attributeErrors: attrResult.errors, indexErrors: idxResult.errors };
    }
}

// Get unique identifier field for a collection (used for missing record detection)
function getUniqueIdentifierField(collectionId) {
    // Define which field uniquely identifies records in each collection
    // Add more mappings as needed for your collections
    const identifierMap = {
        'packaging_records': 'waybill_number',
        'packaging_items': 'packaging_record_id', // Combined with product_barcode and scanned_at
    };
    return identifierMap[collectionId] || null;
}

// Build a set of existing record identifiers from destination for comparison
async function buildExistingRecordSet(databases, destDbId, collectionId) {
    const existingDocs = await fetchAllDocuments(databases, destDbId, collectionId);
    const identifierField = getUniqueIdentifierField(collectionId);

    const existingSet = new Set();
    for (const doc of existingDocs) {
        if (identifierField && doc[identifierField]) {
            // Use the unique identifier field if available
            existingSet.add(doc[identifierField]);
        } else {
            // Fall back to using $id (source document ID stored in a field, if available)
            // Or create a hash of key fields
            const key = JSON.stringify(cleanDocumentData(doc));
            existingSet.add(key);
        }
    }
    return { existingSet, identifierField };
}

// Check if a document already exists in destination
function isDocumentMissing(document, existingSet, identifierField) {
    if (identifierField && document[identifierField]) {
        return !existingSet.has(document[identifierField]);
    } else {
        const key = JSON.stringify(cleanDocumentData(document));
        return !existingSet.has(key);
    }
}

// Clone entire database (structure + data)
async function cloneDatabase(databases, sourceDbId, destDbId, options = {}) {
    const { cloneStructure = true, cloneData = true, missingOnly = false } = options;

    const results = {
        collections: { success: 0, failed: 0, errors: [], attributeErrors: [] },
        documents: { success: 0, failed: 0, skipped: 0, errors: [] }
    };

    // Fetch all collections from source
    console.log('\nFetching collections from source database...');
    const collections = await fetchAllCollections(databases, sourceDbId);

    if (collections.length === 0) {
        console.log('No collections found in source database.');
        return results;
    }

    console.log(`Found ${collections.length} collections to clone.`);

    // Clone collection structures first
    if (cloneStructure) {
        console.log('\n' + styleText('cyan', '--- Cloning Collection Structures ---'));

        const structureBar = createProgressBar(
            '  Structures |{bar}| {percentage}% | {value}/{total} | {collection}',
            collections.length
        );

        for (const collection of collections) {
            structureBar.update({ collection: collection.name.substring(0, 20).padEnd(20) });
            const result = await cloneCollectionStructure(databases, sourceDbId, destDbId, collection, { silent: true });
            if (result.success) {
                results.collections.success++;
            } else {
                results.collections.failed++;
                results.collections.errors.push({
                    collection: collection.name,
                    error: result.error
                });
                // Track attribute errors separately for detailed reporting
                if (result.attributeErrors && result.attributeErrors.length > 0) {
                    results.collections.attributeErrors.push({
                        collection: collection.name,
                        errors: result.attributeErrors
                    });
                }
            }
            structureBar.increment();
        }

        structureBar.stop();
        console.log(`  Completed: ${results.collections.success} success, ${results.collections.failed} failed`);
    }

    // Clone documents
    if (cloneData || missingOnly) {
        console.log('\n' + styleText('cyan', missingOnly ? '--- Adding Missing Documents ---' : '--- Cloning Documents ---'));

        // Step 1: Fetch all documents from source and save to JSON cache
        console.log('\n  Step 1: Fetching all documents from source...');
        let totalDocs = 0;
        const collectionDocs = [];

        for (const collection of collections) {
            const docs = await fetchAllDocuments(databases, sourceDbId, collection.$id);
            totalDocs += docs.length;
            collectionDocs.push({
                collectionId: collection.$id,
                collectionName: collection.name,
                documents: docs
            });
            console.log(`    ${collection.name}: ${docs.length} documents`);
        }

        console.log(`  Total documents fetched: ${totalDocs}`);

        // Write to JSON cache file
        const cacheData = {
            sourceDbId,
            destDbId,
            fetchedAt: new Date().toISOString(),
            collections: collectionDocs.map(cd => ({
                collectionId: cd.collectionId,
                collectionName: cd.collectionName,
                documentCount: cd.documents.length,
                documents: cd.documents
            }))
        };
        writeCacheFile(cacheData);

        // Step 2: Read from JSON cache and insert documents
        console.log('\n  Step 2: Reading from cache and inserting documents...');
        const cachedData = readCacheFile();

        // Calculate total documents to insert
        let docsToProcess = 0;
        const processQueue = [];

        for (const collData of cachedData.collections) {
            if (missingOnly) {
                // Build set of existing records for this collection
                console.log(`    Checking existing records in ${collData.collectionName}...`);
                const { existingSet, identifierField } = await buildExistingRecordSet(
                    databases, destDbId, collData.collectionId
                );

                const missingDocs = collData.documents.filter(doc =>
                    isDocumentMissing(doc, existingSet, identifierField)
                );

                console.log(`    ${collData.collectionName}: ${missingDocs.length} missing of ${collData.documents.length} total`);
                docsToProcess += missingDocs.length;
                results.documents.skipped += (collData.documents.length - missingDocs.length);
                processQueue.push({
                    collectionId: collData.collectionId,
                    collectionName: collData.collectionName,
                    documents: missingDocs
                });
            } else {
                docsToProcess += collData.documents.length;
                processQueue.push({
                    collectionId: collData.collectionId,
                    collectionName: collData.collectionName,
                    documents: collData.documents
                });
            }
        }

        console.log(`\n  Documents to insert: ${docsToProcess}`);
        if (missingOnly) {
            console.log(`  Documents skipped (already exist): ${results.documents.skipped}`);
        }

        if (docsToProcess > 0) {
            const docBar = createProgressBar(
                '  Documents  |{bar}| {percentage}% | {value}/{total} | {collection}',
                docsToProcess
            );

            for (const { collectionId, collectionName, documents } of processQueue) {
                if (documents.length === 0) continue;

                docBar.update({ collection: collectionName.substring(0, 20).padEnd(20) });

                for (const document of documents) {
                    const cleanedData = cleanDocumentData(document);

                    try {
                        await databases.createDocument(
                            destDbId,
                            collectionId,
                            ID.unique(),
                            cleanedData
                        );
                        results.documents.success++;
                    } catch (error) {
                        results.documents.failed++;
                        if (!results.documents.errors.find(e => e.collection === collectionName)) {
                            results.documents.errors.push({ collection: collectionName, errors: [] });
                        }
                        const collErrors = results.documents.errors.find(e => e.collection === collectionName);
                        collErrors.errors.push({ documentId: document.$id, error: error.message });
                    }

                    docBar.increment();
                }
            }

            docBar.stop();
        }

        console.log(`  Completed: ${results.documents.success} success, ${results.documents.failed} failed`);
        if (missingOnly) {
            console.log(`  Skipped (already exist): ${results.documents.skipped}`);
        }

        // Step 3: Delete cache file
        deleteCacheFile();
    }

    return results;
}

// Fetch all collections from a database
async function fetchAllCollections(databases, databaseId) {
    const collections = [];
    let lastId = null;
    let hasMore = true;

    while (hasMore) {
        const queries = [Query.limit(100)];

        if (lastId) {
            queries.push(Query.cursorAfter(lastId));
        }

        const response = await databases.listCollections(databaseId, queries);
        collections.push(...response.collections);

        if (response.collections.length < 100) {
            hasMore = false;
        } else {
            lastId = response.collections[response.collections.length - 1].$id;
        }
    }

    return collections;
}

// Fetch all documents from a collection with pagination
async function fetchAllDocuments(databases, databaseId, collectionId) {
    const documents = [];
    let lastId = null;
    let hasMore = true;

    while (hasMore) {
        const queries = [Query.limit(config.batchSize)];

        if (lastId) {
            queries.push(Query.cursorAfter(lastId));
        }

        const response = await databases.listDocuments(databaseId, collectionId, queries);
        documents.push(...response.documents);

        if (response.documents.length < config.batchSize) {
            hasMore = false;
        } else {
            lastId = response.documents[response.documents.length - 1].$id;
        }
    }

    return documents;
}

// Check if an object is a related document (has Appwrite system fields)
function isRelatedDocument(obj) {
    return obj !== null &&
           typeof obj === 'object' &&
           !Array.isArray(obj) &&
           '$id' in obj &&
           '$collectionId' in obj;
}

// Clean document data by removing Appwrite system fields
// For relationship fields, extract only the $id reference(s)
function cleanDocumentData(document) {
    const systemFields = [
        '$id',
        '$collectionId',
        '$databaseId',
        '$createdAt',
        '$updatedAt',
        '$permissions',
        '$sequence',
    ];

    const cleanedData = {};

    for (const [key, value] of Object.entries(document)) {
        if (!systemFields.includes(key)) {
            // If value is an array (could be relationship data)
            if (Array.isArray(value)) {
                cleanedData[key] = value.map(item => {
                    // If item is a related document, extract just the $id
                    if (isRelatedDocument(item)) {
                        return item.$id;
                    }
                    // If item is an object with any system fields, recursively clean it
                    if (item !== null && typeof item === 'object' && !Array.isArray(item)) {
                        return cleanNestedObject(item, systemFields);
                    }
                    return item;
                });
            }
            // If value is a related document, extract just the $id
            else if (isRelatedDocument(value)) {
                cleanedData[key] = value.$id;
            }
            // If value is an object, recursively clean it to remove any system fields
            else if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                cleanedData[key] = cleanNestedObject(value, systemFields);
            }
            else {
                cleanedData[key] = value;
            }
        }
    }

    return cleanedData;
}

// Recursively clean nested objects by removing system fields
function cleanNestedObject(obj, systemFields) {
    const cleaned = {};
    for (const [key, value] of Object.entries(obj)) {
        if (!systemFields.includes(key)) {
            if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                cleaned[key] = cleanNestedObject(value, systemFields);
            } else if (Array.isArray(value)) {
                cleaned[key] = value.map(item => {
                    if (item !== null && typeof item === 'object' && !Array.isArray(item)) {
                        return cleanNestedObject(item, systemFields);
                    }
                    return item;
                });
            } else {
                cleaned[key] = value;
            }
        }
    }
    return cleaned;
}

// Copy documents to destination collection
async function copyDocuments(databases, destDatabaseId, collectionId, documents) {
    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    for (const document of documents) {
        const cleanedData = cleanDocumentData(document);

        try {
            await databases.createDocument(
                destDatabaseId,
                collectionId,
                ID.unique(),
                cleanedData
            );
            successCount++;
        } catch (error) {
            errorCount++;
            errors.push({
                documentId: document.$id,
                error: error.message,
            });
        }
    }

    return { successCount, errorCount, errors };
}

// Copy a single collection
async function copyCollection(databases, collection) {
    const collectionId = collection.$id;
    const collectionName = collection.name;

    console.log(`\n  Collection: ${collectionName} (${collectionId})`);

    // Fetch documents from source
    const documents = await fetchAllDocuments(
        databases,
        config.sourceDatabaseId,
        collectionId
    );

    if (documents.length === 0) {
        console.log(`    No documents found, skipping...`);
        return { collectionName, successCount: 0, errorCount: 0, errors: [] };
    }

    console.log(`    Found ${documents.length} documents, copying...`);

    // Copy documents to destination
    const result = await copyDocuments(
        databases,
        config.destDatabaseId,
        collectionId,
        documents
    );

    console.log(`    Copied: ${result.successCount}, Failed: ${result.errorCount}`);

    return { collectionName, ...result };
}

// Main function
async function main() {
    console.log('='.repeat(60));
    console.log(styleText('bold', '  Appwrite Database Full Clone Tool'));
    console.log('='.repeat(60));

    validateConfig();

    const client = createClient();
    const databases = new Databases(client);

    try {
        // Get database info for confirmation
        console.log('\nFetching database information...');
        const [sourceDb, destDb] = await Promise.all([
            getDatabaseInfo(databases, config.sourceDatabaseId),
            getDatabaseInfo(databases, config.destDatabaseId),
        ]);

        // Validate source database exists
        if (!sourceDb.exists) {
            console.error(styleText('red', `\nError: Source database "${config.sourceDatabaseId}" not found.`));
            process.exit(1);
        }

        // Select clone mode interactively first (to determine if we need destination validation)
        const cloneMode = await selectCloneMode();

        // Handle CSV export mode separately (doesn't need destination)
        if (cloneMode === 'export-csv') {
            console.log('\n' + '='.repeat(60));
            console.log(styleText('cyan', '  Starting CSV Export'));
            console.log('='.repeat(60));

            console.log(`\n  Source: ${sourceDb.name} (${sourceDb.id})`);

            // Ask if user wants to include system fields
            const includeSystemFields = await confirm({
                message: 'Include system fields ($id, $createdAt, etc.) in CSV? (Note: Must be "No" if re-importing into Appwrite)',
                default: false,
            });

            // Perform CSV export
            const csvResults = await exportToCSV(databases, config.sourceDatabaseId, includeSystemFields);

            // Print summary
            console.log('\n' + '='.repeat(60));
            console.log(styleText('green', styleText('bold', '  CSV Export Complete!')));
            console.log('='.repeat(60));

            console.log(`\n  Export directory: ${styleText('cyan', csvResults.exportDir)}`);
            console.log(`  Total records:    ${styleText('green', String(csvResults.totalRecords))}`);
            console.log(`\n  Files created:`);

            for (const coll of csvResults.collections) {
                if (coll.recordCount > 0) {
                    console.log(`    ${styleText('green', '✓')} ${coll.filename} (${coll.recordCount} records)`);
                } else {
                    console.log(`    ${styleText('yellow', '-')} ${coll.name}: No records to export`);
                }
            }

            console.log('');
            return;
        }

        // Validate destination database exists (for non-export modes)
        if (!destDb.exists) {
            console.error(styleText('red', `\nError: Destination database "${config.destDatabaseId}" not found.`));
            process.exit(1);
        }

        // Confirm migration with user
        await confirmMigration(sourceDb, destDb);

        // Determine clone options based on mode
        const cloneOptions = {
            cloneStructure: cloneMode === 'full' || cloneMode === 'structure-only',
            cloneData: cloneMode === 'full' || cloneMode === 'data-only',
            missingOnly: cloneMode === 'missing-only',
        };

        console.log('\n' + '='.repeat(60));
        console.log(styleText('cyan', '  Starting Migration'));
        console.log('='.repeat(60));

        console.log(`\n  Source:      ${sourceDb.name} (${sourceDb.id})`);
        console.log(`  Destination: ${destDb.name} (${destDb.id})`);
        console.log(`  Mode:        ${cloneMode}`);
        console.log(`  Batch size:  ${config.batchSize}`);

        // Drop destination collections first (unless data-only or missing-only mode)
        if (cloneOptions.cloneStructure && !cloneOptions.missingOnly) {
            await dropDestinationCollections(databases, config.destDatabaseId);
        }

        // Perform full database clone
        const results = await cloneDatabase(
            databases,
            config.sourceDatabaseId,
            config.destDatabaseId,
            cloneOptions
        );

        // Print summary
        console.log('\n' + '='.repeat(60));
        console.log(styleText('green', styleText('bold', '  Clone Complete!')));
        console.log('='.repeat(60));

        if (cloneOptions.cloneStructure) {
            console.log(`\n  Collections cloned: ${styleText('green', String(results.collections.success))}`);
            if (results.collections.failed > 0) {
                console.log(`  Collections failed: ${styleText('red', String(results.collections.failed))}`);
            }
        }

        if (cloneOptions.cloneData || cloneOptions.missingOnly) {
            console.log(`\n  Documents copied:   ${styleText('green', String(results.documents.success))}`);
            if (results.documents.failed > 0) {
                console.log(`  Documents failed:   ${styleText('red', String(results.documents.failed))}`);
            }
            if (cloneOptions.missingOnly && results.documents.skipped > 0) {
                console.log(`  Documents skipped:  ${styleText('yellow', String(results.documents.skipped))} (already exist)`);
            }
        }

        // Print detailed collection errors if any
        if (results.collections.errors.length > 0) {
            console.log('\n' + styleText('red', 'Collection errors:'));
            for (const err of results.collections.errors) {
                console.log(`  - ${err.collection}: ${err.error}`);
            }
        }

        // Print detailed attribute errors if any
        if (results.collections.attributeErrors && results.collections.attributeErrors.length > 0) {
            console.log('\n' + styleText('red', 'Attribute errors by collection:'));
            for (const collErr of results.collections.attributeErrors) {
                console.log(`\n  ${collErr.collection}:`);
                for (const err of collErr.errors) {
                    console.log(`    - ${err.key}: ${err.error}`);
                }
            }
        }

        // Print detailed document errors if any
        if (results.documents.errors.length > 0) {
            console.log('\n' + styleText('red', 'Document errors by collection:'));
            for (const collErr of results.documents.errors) {
                console.log(`\n  ${collErr.collection}:`);
                for (const err of collErr.errors) {
                    console.log(`    - Document ${err.documentId}: ${err.error}`);
                }
            }
        }

        console.log('');
    } catch (error) {
        if (error.name === 'ExitPromptError') {
            console.log('\nMigration cancelled by user.');
            process.exit(0);
        }
        console.error('\n' + styleText('red', `Error during clone operation: ${error.message}`));
        process.exit(1);
    }
}

// Run the application
main();
