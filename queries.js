// queries.js - MongoDB queries for the Week 1 Assignment

const { MongoClient } = require('mongodb');

// Connection URI 
const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runQueries() {
    const client = new MongoClient(uri);

    try {
        await client.connect();
        console.log('Connected to MongoDB');
        
        const db = client.db(dbName);
        const books = db.collection(collectionName);

        // Task 2: Basic CRUD Operations
        console.log('\n--- Task 2: Basic CRUD Operations ---');

        // Find all books in a specific genre (e.g., Fantasy)
        console.log('\nBooks in Fantasy genre:');
        const fantasyBooks = await books.find({ genre: 'Fantasy' }).toArray();
        console.log(fantasyBooks);

        // Find books published after a certain year (e.g., 2000)
        console.log('\nBooks published after 2000:');
        const recentBooks = await books.find({ published_year: { $gt: 2000 } }).toArray();
        console.log(recentBooks);

        // Find books by a specific author
        console.log('\nBooks by J.R.R. Tolkien:');
        const tolkienBooks = await books.find({ author: 'J.R.R. Tolkien' }).toArray();
        console.log(tolkienBooks);

        // Update the price of a specific book
        console.log('\nUpdating price of "The Hobbit":');
        const updateResult = await books.updateOne(
            { title: 'The Hobbit' },
            { $set: { price: 15.99 } }
        );
        console.log(updateResult);

        // Delete a book by its title (Note: Add a test book first)
        console.log('\nAdding and then deleting a test book:');
        await books.insertOne({
            title: 'Test Book',
            author: 'Test Author',
            genre: 'Test',
            published_year: 2023,
            price: 9.99,
            in_stock: true,
            pages: 100,
            publisher: 'Test Publisher'
        });
        const deleteResult = await books.deleteOne({ title: 'Test Book' });
        console.log(deleteResult);

        // Task 3: Advanced Queries
        console.log('\n--- Task 3: Advanced Queries ---');

        // Find books that are both in stock and published after 2010
        console.log('\nBooks in stock and published after 2010:');
        const inStockRecent = await books.find({
            in_stock: true,
            published_year: { $gt: 2010 }
        }).toArray();
        console.log(inStockRecent);

        // Projection to return only title, author, and price
        console.log('\nBooks with selected fields:');
        const projectedBooks = await books.find(
            {},
            { projection: { _id: 0, title: 1, author: 1, price: 1 } }
        ).toArray();
        console.log(projectedBooks);

        // Sort books by price (ascending)
        console.log('\nBooks sorted by price (ascending):');
        const sortedBooksAsc = await books.find()
            .sort({ price: 1 })
            .toArray();
        console.log(sortedBooksAsc);

        // Sort books by price (descending)
        console.log('\nBooks sorted by price (descending):');
        const sortedBooksDesc = await books.find()
            .sort({ price: -1 })
            .toArray();
        console.log(sortedBooksDesc);

        // Pagination (5 books per page)
        console.log('\nFirst page (5 books):');
        const page1 = await books.find()
            .limit(5)
            .toArray();
        console.log(page1);

        console.log('\nSecond page (5 books):');
        const page2 = await books.find()
            .skip(5)
            .limit(5)
            .toArray();
        console.log(page2);

        // Task 4: Aggregation Pipeline
        console.log('\n--- Task 4: Aggregation Pipeline ---');

        // Average price of books by genre
        console.log('\nAverage price by genre:');
        const avgPriceByGenre = await books.aggregate([
            {
                $group: {
                    _id: '$genre',
                    averagePrice: { $avg: '$price' }
                }
            }
        ]).toArray();
        console.log(avgPriceByGenre);

        // Author with the most books
        console.log('\nAuthor with most books:');
        const authorWithMostBooks = await books.aggregate([
            {
                $group: {
                    _id: '$author',
                    bookCount: { $sum: 1 }
                }
            },
            {
                $sort: { bookCount: -1 }
            },
            {
                $limit: 1
            }
        ]).toArray();
        console.log(authorWithMostBooks);

        // Books grouped by publication decade
        console.log('\nBooks by publication decade:');
        const booksByDecade = await books.aggregate([
            {
                $group: {
                    _id: {
                        $concat: [
                            { $substr: [{ $toString: { $subtract: ['$published_year', { $mod: ['$published_year', 10] }] } }, 0, 4] },
                            '0s'
                        ]
                    },
                    count: { $sum: 1 }
                }
            },
            {
                $sort: { _id: 1 }
            }
        ]).toArray();
        console.log(booksByDecade);

        // Task 5: Indexing
        console.log('\n--- Task 5: Indexing ---');

        // Create index on title field
        console.log('\nCreating index on title field:');
        const titleIndex = await books.createIndex({ title: 1 });
        console.log(titleIndex);

        // Create compound index on author and published_year
        console.log('\nCreating compound index on author and published_year:');
        const compoundIndex = await books.createIndex({ author: 1, published_year: 1 });
        console.log(compoundIndex);

        // Demonstrate performance improvement with explain()
        console.log('\nQuery performance with index:');
        const queryExplain = await books.find({ title: 'The Hobbit' })
            .explain('executionStats');
        console.log(queryExplain);

    } catch (err) {
        console.error('Error:', err);
    } finally {
        await client.close();
        console.log('\nDisconnected from MongoDB');
    }
}

// Run the queries
runQueries().catch(console.error);
