import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ResultsetRow;
import entities.Book;
import entities.Borrow;
import entities.Card;
import queries.*;
import utils.DBInitializer;
import utils.DatabaseConnector;

import javax.swing.text.html.HTMLDocument;
import java.lang.invoke.StringConcatException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class LibraryManagementSystemImpl implements LibraryManagementSystem {

    private final DatabaseConnector connector;

    public LibraryManagementSystemImpl(DatabaseConnector connector) {
        this.connector = connector;
    }

    @Override
    public ApiResult storeBook(Book book) {
        Connection conn = connector.getConn();
        try {
            conn.setAutoCommit(false);
            PreparedStatement stmt = conn.prepareStatement("select * from book " +
                    "where category = ? and title = ? and press = ? and publish_year = ? and author = ?");
            stmt.setString(1, book.getCategory());
            stmt.setString(2, book.getTitle());
            stmt.setString(3, book.getPress());
            stmt.setInt(4, book.getPublishYear());
            stmt.setString(5, book.getAuthor());
            if(stmt.executeQuery().next()) { // if the book is already in the library, throw an exception
                throw new Exception("the book is already in this library");
            }
            else {
                String sql = "INSERT INTO book(category,title,press,publish_year,author,price,stock) VALUES(?,?,?,?,?,?,?)";
                // inserting the new book
                PreparedStatement insert = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

                insert.setString(1, book.getCategory());
                insert.setString(2, book.getTitle());
                insert.setString(3, book.getPress());
                insert.setInt(4, book.getPublishYear());
                insert.setString(5, book.getAuthor());
                insert.setDouble(6, book.getPrice());
                insert.setInt(7, book.getStock());
                int rows = insert.executeUpdate();
                if (rows > 0) {
                    conn.commit();
                    ResultSet rs = insert.getGeneratedKeys();

                    if (rs.next()) { // if the insertion was successful
                        int book_id = rs.getInt(1);
                        book.setBookId(book_id);
                    }
                }

            }
            return new ApiResult(true, null);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult incBookStock(int bookId, int deltaStock) {
        Connection conn = connector.getConn();
        try {
            conn.setAutoCommit(false);
            String sql1 = "select * from book where book_id = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setInt(1, bookId);
            ResultSet rs = stmt.executeQuery();
            int currentStock = 0;
            if (rs.next()){
                currentStock = rs.getInt("stock");
                currentStock += deltaStock;
            } else { // there isn't such a book
                throw new SQLException("the book does not exist");
            }
            if (currentStock < 0) { // if the updated stock is negative
                throw new Exception("the stock cannot be negative");
            }
            else { // if the update is legal
                String sql = "update book set stock = ? where book_id = ?";
                PreparedStatement update = conn.prepareStatement(sql);
                update.setInt(1, currentStock);
                update.setInt(2, bookId);
                update.executeUpdate();
            }
            commit(conn);
            return new ApiResult(true, null);
        } catch (Exception e){
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult storeBook(List<Book> books) {
        Connection conn = connector.getConn();
        try {
            conn.setAutoCommit(false);
            String insertion = "insert into book(category, title, press, publish_year, author, price, stock) " +
                    "values(?,?,?,?,?,?,?)";
            // inserting the new book
            PreparedStatement insertor = conn.prepareStatement(insertion, Statement.RETURN_GENERATED_KEYS);
            for (Book book : books) {
                insertor.setString(1, book.getCategory());
                insertor.setString(2, book.getTitle());
                insertor.setString(3, book.getPress());
                insertor.setInt(4, book.getPublishYear());
                insertor.setString(5, book.getAuthor());
                insertor.setDouble(6, book.getPrice());
                insertor.setInt(7, book.getStock());

                insertor.addBatch();
            }

            int[] rows = insertor.executeBatch();
            if (rows.length > 0) {
                conn.commit();
                ResultSet rs = insertor.getGeneratedKeys();

                // set book_id for books
                int i = 0;
                while (rs.next()) {
                    int book_id = rs.getInt(1);
                    books.get(i).setBookId(book_id);
                    i += 1;
                }
            }
            else {
                conn.rollback();
                throw new SQLException("insert failed");
            }
            return new ApiResult(true, null);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult removeBook(int bookId) {
        Connection conn = connector.getConn();
        try {
            conn.setAutoCommit(false);
            String selection = "select * from book where book_id = ?";
            PreparedStatement selector = conn.prepareStatement(selection);
            selector.setInt(1, bookId);
            ResultSet rs = selector.executeQuery();
            if (rs.next()) { // successfully find the book
                String selection_for_borrowing = "select * from borrow where book_id = ? and return_time < borrow_time";
                PreparedStatement selector_for_borrowing = conn.prepareStatement(selection_for_borrowing);
                selector_for_borrowing.setInt(1, bookId);
                ResultSet rs_for_borrowing = selector_for_borrowing.executeQuery();
                if (rs_for_borrowing.next()) { // the book is borrowed
                    throw new Exception("the book is borrowed by someone and has not been returned");
                }
                else {
                    String deletion = "delete from book where book_id = ?";
                    PreparedStatement deleter = conn.prepareStatement(deletion);
                    deleter.setInt(1, bookId);
                    deleter.executeUpdate();
                    conn.commit();
                    return new  ApiResult(true, null);
                }
            }
            else { // the book wasn't found
                throw new Exception("there isn't such a book.");
            }
        } catch(Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult modifyBookInfo(Book book) {
        Connection conn = connector.getConn();
        try {
            conn.setAutoCommit(false);
            String selection = "select * from book where book_id = ?";
            PreparedStatement selector = conn.prepareStatement(selection);
            selector.setInt(1, book.getBookId());
            ResultSet rs = selector.executeQuery();
            if (rs.next()) { // the book is in the library
                String update = "update book " +
                        "set category = ?, title = ?, press = ?, publish_year = ?, author = ?, price = ?" +
                        "where book_id = ?";
                PreparedStatement updater = conn.prepareStatement(update);
                updater.setString(1, book.getCategory());
                updater.setString(2, book.getTitle());
                updater.setString(3, book.getPress());
                updater.setInt(4, book.getPublishYear());
                updater.setString(5, book.getAuthor());
                updater.setDouble(6, book.getPrice());
                updater.setInt(7, book.getBookId());

                updater.executeUpdate();

                commit(conn);
                return new  ApiResult(true, null);
            }
            else { // the book isn't in the library
                throw new Exception("the book does not exist");
            }
        } catch(Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult queryBook(BookQueryConditions conditions) { // undone
        Connection conn = connector.getConn();
        try {
            Boolean hasorder = Boolean.FALSE;
            StringBuilder select = new StringBuilder("select * from book ");
            List<Object> params = new ArrayList<>(); // avoid SQL injection
            Book.SortColumn orderby = null;
            String sortOrder;
            select.append(" where true ");
            if (conditions.getCategory() != null) {
                select.append(" and category = ? ");
                params.add(conditions.getCategory());
            }
            if (conditions.getTitle() != null) {
                select.append(" and title like ? ");
                params.add("%" + conditions.getTitle() + "%");
            }
            if (conditions.getPress() != null) {
                select.append(" and press like ? ");
                params.add("%" + conditions.getPress() + "%");
            }
            if (conditions.getMinPublishYear() != null) {
                select.append(" and publish_year >= ? ");
                params.add(conditions.getMinPublishYear());
            }
            if (conditions.getMaxPublishYear() != null) {
                select.append(" and publish_year <= ? ");
                params.add(conditions.getMaxPublishYear());
            }
            if (conditions.getAuthor() != null) {
                select.append(" and author like ? ");
                params.add("%" + conditions.getAuthor() + "%");
            }
            if (conditions.getMinPrice() != null) {
                select.append(" and price >= ? ");
                params.add(conditions.getMinPrice());
            }
            if (conditions.getMaxPrice() != null) {
                select.append(" and price <= ? ");
                params.add(conditions.getMaxPrice());
            }
            if (conditions.getSortBy() != null) {
                hasorder = Boolean.TRUE;
                select.append(" order by " + conditions.getSortBy() + " ");
                if (conditions.getSortOrder() != null) {
                    sortOrder = conditions.getSortOrder().getValue();
                    select.append(" " + sortOrder +" , book_id ASC ");
                }
                else {
                    select.append(" , book_id ASC ");
                }
            }
            else {
                if (conditions.getSortOrder() != null) {
                    select.append(" order by book_id ");
                }
                select.append(" order by book_id ASC ");
            }
            PreparedStatement selector = conn.prepareStatement(select.toString());
            int i = 0;
            for (i = 0 ; i < params.size(); i++) {
                selector.setObject(i + 1, params.get(i));
            }
//            if (hasorder == Boolean.TRUE) {
//                selector.setString(i + 1, orderby.getValue());
//            }
            ResultSet rs = selector.executeQuery();
            List<Book> books = new ArrayList<>();
            while (rs.next()) {
                Book book = new Book();

                book.setBookId(rs.getInt("book_id"));
                book.setCategory(rs.getString("category"));
                book.setTitle(rs.getString("title"));
                book.setPress(rs.getString("press"));
                book.setPublishYear(rs.getInt("publish_year"));
                book.setAuthor(rs.getString("author"));
                book.setPrice(rs.getDouble("price"));
                book.setStock(rs.getInt("stock"));

                books.add(book);
            }
            BookQueryResults bookQueryResults = new BookQueryResults(books);
            return new ApiResult(true, bookQueryResults);

        } catch(Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult borrowBook(Borrow borrow) {
        Connection conn = connector.getConn();
        try {
            synchronized (LOCK) {
                conn.setAutoCommit(false);
                String selection_from_borrow = "select * from borrow where book_id = ? and card_id = ? and return_time = 0";
                String selection_from_book = "select * from book where book_id = ?";
                PreparedStatement selector_from_borrow = conn.prepareStatement(selection_from_borrow);
                PreparedStatement selector_from_book = conn.prepareStatement(selection_from_book);
                selector_from_borrow.setInt(1, borrow.getBookId());
                selector_from_borrow.setInt(2, borrow.getCardId());
                selector_from_book.setInt(1, borrow.getBookId());
                ResultSet rs_from_borrow = selector_from_borrow.executeQuery();
                ResultSet rs_from_book = selector_from_book.executeQuery();;
                if (rs_from_borrow.next()) {
                    throw new Exception("the book is borrowed by this card and has not been returned");
                }
                else {
                    if (rs_from_book.next()) { // the book exists, check storage
                        if (rs_from_book.getInt("stock") < 1) { // the stock is not enough
                            throw new Exception("there has no more books of this id left");
                        } else {
                            // insert into the borrow record
                            String insert = "insert into borrow(book_id, card_id, borrow_time) values(?,?,?)";
                            PreparedStatement inserter = conn.prepareStatement(insert);
                            inserter.setInt(1, borrow.getBookId());
                            inserter.setInt(2, borrow.getCardId());
                            inserter.setLong(3, borrow.getBorrowTime());
                            inserter.executeUpdate();

                            // update the book record
                            String update = "update book " +
                                    "set stock = stock - 1 " +
                                    "where book_id = ?";
                            PreparedStatement updater = conn.prepareStatement(update);
                            updater.setInt(1, borrow.getBookId());
                            updater.executeUpdate();

                            commit(conn);
                            return new ApiResult(true, null);
                        }
                    } else { // the book doesn't exist
                        throw new Exception("the book does not exist");
                    }
                }
            }
        } catch(Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult returnBook(Borrow borrow) {
        Connection conn = connector.getConn();
        try {
            conn.setAutoCommit(false);
            String select = "select * from borrow " +
                    "where book_id = ? and card_id = ? and borrow_time = ? and return_time = 0";
            PreparedStatement selector = conn.prepareStatement(select);
            selector.setInt(1, borrow.getBookId());
            selector.setInt(2, borrow.getCardId());
            selector.setLong(3, borrow.getBorrowTime());
            ResultSet rs_for_borrow = selector.executeQuery();
            if (rs_for_borrow.next()) { // there exist such a book and has not been returned
                // update borrow return time

                String update_for_borrow = "update borrow set return_time = ? where book_id = ? and card_id = ? and borrow_time = ?";
                PreparedStatement updater = conn.prepareStatement(update_for_borrow);
                updater.setLong(1, borrow.getReturnTime());
                updater.setInt(2, borrow.getBookId());
                updater.setInt(3, borrow.getCardId());
                updater.setLong(4, borrow.getBorrowTime());
                updater.executeUpdate();
                conn.commit();


                // update book stock
                String update_for_book = "update book set stock = stock + 1 where book_id = ?";
                PreparedStatement updater_for_book = conn.prepareStatement(update_for_book);
                updater_for_book.setInt(1, borrow.getBookId());
                updater_for_book.executeUpdate();
                commit(conn);

                return new  ApiResult(true, null);
            }
            else {
                throw new Exception("the book does not exist or has been returned");
            }
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult showBorrowHistory(int cardId) {
        Connection conn = connector.getConn();
        try {
            String select = "select * from borrow natural join book natural join card where card_id = ? " +
                    "order by borrow_time desc";
            PreparedStatement selector = conn.prepareStatement(select);
            selector.setInt(1, cardId);
            ResultSet rs_for_select = selector.executeQuery();

            List<BorrowHistories.Item> items = new ArrayList<>();
            while (rs_for_select.next()) {
                Book book = new Book();
                book.setBookId(rs_for_select.getInt("book_id"));
                book.setTitle(rs_for_select.getString("title"));
                book.setAuthor(rs_for_select.getString("author"));
                book.setPrice(rs_for_select.getDouble("price"));
                book.setPress(rs_for_select.getString("press"));
                book.setCategory(rs_for_select.getString("category"));
                book.setPublishYear(rs_for_select.getInt("publish_year"));
                Borrow borrow = new Borrow();
                borrow.setBorrowTime(rs_for_select.getLong("borrow_time"));
                borrow.setReturnTime(rs_for_select.getLong("return_time"));

                BorrowHistories.Item item = new BorrowHistories.Item(cardId, book, borrow);
                items.add(item);
            }
            BorrowHistories histories = new BorrowHistories(items);
            commit(conn);
            return new ApiResult(true, histories);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult registerCard(Card card) {
        Connection conn = connector.getConn();;
        try {
            // first search for the card to check whether it exists or not
            conn.setAutoCommit(false);
            String select = "select * from card where name = ? and department = ? and type = ?";
            PreparedStatement selector = conn.prepareStatement(select);
            selector.setString(1, card.getName());
            selector.setString(2, card.getDepartment());
            selector.setString(3, card.getType().getStr());
            ResultSet rs_for_card_search = selector.executeQuery();
            if (rs_for_card_search.next()) { // the card exist
                throw new Exception("the card already exists");
            }
            else { // the card does not exist
                String insert = "insert into card(name, department, type) values(?,?,?)";
                PreparedStatement inserter = conn.prepareStatement(insert);
                inserter.setString(1, card.getName());
                inserter.setString(2, card.getDepartment());
                inserter.setString(3, card.getType().getStr());
                inserter.executeUpdate();

                rs_for_card_search = selector.executeQuery();
                if (rs_for_card_search.next()) { // the insertion is successful
                    int card_id = rs_for_card_search.getInt("card_id");
                    card.setCardId(card_id);
                    commit(conn);
                    return new  ApiResult(true, null);
                }
                else {
                    throw new Exception("the insertion wasn't successful");
                }
            }
        } catch(Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult removeCard(int cardId) {
        Connection conn = connector.getConn();
        try {
            conn.setAutoCommit(false);
            String select = "select * from card where card_id = ?";
            PreparedStatement selector = conn.prepareStatement(select);
            selector.setInt(1, cardId);
            ResultSet rs_for_card_search = selector.executeQuery();
            if (rs_for_card_search.next()) { // there is such a card
                // check if the card has some books that haven't been returned
                String select_from_borrow = "select * from borrow where card_id = ? and return_time = 0";
                PreparedStatement selector_from_borrow = conn.prepareStatement(select_from_borrow);
                selector_from_borrow.setInt(1, cardId);
                rs_for_card_search = selector_from_borrow.executeQuery();
                if (rs_for_card_search.next()) { // the card has some book that haven't returned
                    throw new Exception("the card has some book that haven't been returned");
                }
                else { // all the books have been returned
                    String delete = "delete from card where card_id = ?";
                    PreparedStatement deleter = conn.prepareStatement(delete);
                    deleter.setInt(1, cardId);
                    deleter.executeUpdate();
                    commit(conn);
                    return new ApiResult(true, null);
                }
            }
            else {
                throw new Exception("the card does not exist");
            }
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult showCards() {
        Connection conn = connector.getConn();
        try {
            String sql = "select card_id, name, department, type from card order by card_id";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            commit(conn);
            List<Card> cards = new ArrayList<>();
            while (rs.next()) {
                Card card = new Card();
                card.setCardId(rs.getInt(1));
                card.setName(rs.getString(2));
                card.setDepartment(rs.getString(3));
                card.setType(Card.CardType.values(rs.getString(4)));
                cards.add(card);
            }
            return new ApiResult(true, new CardList(cards));
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult resetDatabase() {
        Connection conn = connector.getConn();
        try {
            Statement stmt = conn.createStatement();
            DBInitializer initializer = connector.getConf().getType().getDbInitializer();
            stmt.addBatch(initializer.sqlDropBorrow());
            stmt.addBatch(initializer.sqlDropBook());
            stmt.addBatch(initializer.sqlDropCard());
            stmt.addBatch(initializer.sqlCreateCard());
            stmt.addBatch(initializer.sqlCreateBook());
            stmt.addBatch(initializer.sqlCreateBorrow());
            stmt.executeBatch();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    private void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void commit(Connection conn) {
        try {
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final Object LOCK = new Object();
}

