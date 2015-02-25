package cn.edu.sjtu.acm.jdbctaste.dao.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import cn.edu.sjtu.acm.jdbctaste.dao.CommentDao;
import cn.edu.sjtu.acm.jdbctaste.dao.JokeDao;
import cn.edu.sjtu.acm.jdbctaste.dao.PersonDao;
import cn.edu.sjtu.acm.jdbctaste.entity.Comment;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;

public class SqliteCommentDao implements CommentDao {

	private final Connection conn;
	
	public static final int IDX_ID = 1, IDX_BODY = 2, IDX_JOKE = 3, IDX_COMMENTATOR = 4, IDX_POST_TIME = 5;
	
	public SqliteCommentDao(Connection conn) {
		this.conn = conn;
	}

	@Override
	public int insertComment(Comment comment) {
		int ret = -1;

		try {
			PreparedStatement stat = conn.prepareStatement(
					"insert into comment (joke, commentator, body, posttime) values (?,?,?,?);",
					Statement.RETURN_GENERATED_KEYS);
			stat.setInt(1, comment.getJoke().getId());
			stat.setInt(2, comment.getCommentator().getId());
			stat.setString(3, comment.getBody());
			stat.setTimestamp(4, comment.getPostTime());
			
			stat.executeUpdate();
			
			ResultSet rs = stat.getGeneratedKeys();
			
			if (rs.next()) {
				int id = rs.getInt(1);
				comment.setId(id);
				ret = id;
			}

			rs.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
			ret = -1;
		}

		return ret;
	}

	@Override
	public boolean deleteComment(Comment comment) {
		// cqm
		boolean ret = false;
		try {
			PreparedStatement stat = conn.prepareStatement(
					"delete from comment where id = ?;");
			stat.setInt(1, comment.getId());

			stat.executeUpdate();
			ret = true;

			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
			ret = false;
		}
		return ret;
	}

	@Override
	public boolean updateComment(Comment comment) {
		// cqm
		boolean ret = false;
		try {
			PreparedStatement stat = conn.prepareStatement(
					"update comment set joke = ?, commentator = ?, body = ?, posttime = ? where id = ?;");
			stat.setInt(1, comment.getJoke().getId());
			stat.setInt(2, comment.getCommentator().getId());
			stat.setString(3, comment.getBody());
			stat.setTimestamp(4, comment.getPostTime());
			stat.setInt(5, comment.getId());
			int count = stat.executeUpdate();
			if (count != 0)
				ret = true;
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
			ret = false;
		}
		return ret;
	}

	@Override
	public List<Comment> findCommentsOfPerson(Person person) {
		// cqm
		List<Comment> ret = new LinkedList<Comment>();
		
		try {
			PreparedStatement stat = conn
					.prepareStatement("select * from comment where commentator = ?;");
			stat.setInt(1, person.getId());
			ResultSet result = stat.executeQuery();

			PersonDao personDao = SqliteDaoFactory.getInstance().getPersonDao();
			JokeDao jokeDao = SqliteDaoFactory.getInstance().getJokeDao();
			
			Person commentator;
			Joke joke;
			
			while (result.next()) 
			{
				commentator = personDao.findPersonById(result.getInt(IDX_COMMENTATOR));	
				joke = jokeDao.findJokeById(result.getInt(IDX_JOKE));
				ret.add(new Comment(result.getInt(IDX_ID), joke, commentator, result.getString(IDX_BODY), 
						result.getTimestamp(IDX_POST_TIME)));
			}
			result.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return ret;
	}

	@Override
	public List<Comment> findCommentsReceived(Person person) {
		// cqm
		List<Comment> ret = new LinkedList<Comment>();
		
		try {
			PreparedStatement stat = conn
					.prepareStatement("select comment.* from comment, joke where joke.id = comment.joke and joke.speaker = ?");
			stat.setInt(1, person.getId());
			ResultSet result = stat.executeQuery();
			
			PersonDao personDao = SqliteDaoFactory.getInstance().getPersonDao();
			JokeDao jokeDao = SqliteDaoFactory.getInstance().getJokeDao();
			
			Person commentator;
			Joke joke;
			while (result.next()) 
			{
				commentator = personDao.findPersonById(result.getInt(IDX_COMMENTATOR));
				joke = jokeDao.findJokeById(result.getInt(IDX_JOKE));
				ret.add(new Comment(result.getInt(IDX_ID), joke, commentator, result.getString(IDX_BODY), 
						result.getTimestamp(IDX_POST_TIME)));
			}
			result.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public List<Comment> findCommentsOfJoke(Joke joke) {
		// cqm
		List<Comment> ret = new LinkedList<Comment>();

		try {
			PreparedStatement stat = conn
					.prepareStatement("select * from comment where joke = ?");
			stat.setInt(1, joke.getId());
			ResultSet result = stat.executeQuery();
			
			JokeDao jokeDao = SqliteDaoFactory.getInstance().getJokeDao();
			PersonDao personDao = SqliteDaoFactory.getInstance().getPersonDao();
			
			Person commentator;
			
			while (result.next()) 
			{
				joke = jokeDao.findJokeById(result.getInt(IDX_JOKE));
				commentator = personDao.findPersonById(result.getInt(IDX_COMMENTATOR));
			
				ret.add(new Comment(result.getInt(IDX_ID), joke, commentator, 
						result.getString(IDX_BODY), result.getTimestamp(IDX_POST_TIME)));
			}
			result.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public List<Comment> getAllComments() {
		// cqm
		List<Comment> ret = new LinkedList<Comment>();

		Statement stat;
		try {
			stat = conn.createStatement();

			stat.execute("select * from comment;");
			ResultSet result = stat.getResultSet();

			JokeDao jokeDao = SqliteDaoFactory.getInstance().getJokeDao();
			PersonDao personDao = SqliteDaoFactory.getInstance().getPersonDao();
			
			Joke joke;
			Person commentator;
			
			while (result.next()) 
			{
				joke = jokeDao.findJokeById(result.getInt(IDX_JOKE));
				commentator = personDao.findPersonById(result.getInt(IDX_COMMENTATOR));
			
				ret.add(new Comment(result.getInt(IDX_ID), joke, commentator, 
						result.getString(IDX_BODY), result.getTimestamp(IDX_POST_TIME)));
			}
			result.close();
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public Comment findCommentById(int id) {
		// cqm
		Comment ret = null;
		
		PreparedStatement stat;
		try
		{
			stat = conn.prepareStatement("select * from comment where id = ?;");
			stat.setInt(1,id);
			ResultSet rs = stat.executeQuery();
			
			if (!rs.next())
				return null;
			
			JokeDao jokeDao = SqliteDaoFactory.getInstance().getJokeDao();
			Joke joke = jokeDao.findJokeById(rs.getInt(IDX_JOKE));
			
			PersonDao personDao = SqliteDaoFactory.getInstance().getPersonDao();
			Person commentator = personDao.findPersonById(rs.getInt(IDX_COMMENTATOR));
			
			ret = new Comment(rs.getInt(IDX_ID), joke, commentator, rs.getString(IDX_BODY), rs.getTimestamp(IDX_POST_TIME));
			
			rs.close();
			stat.close();
		
		} catch (SQLException e)
		{
			e.printStackTrace();
			return null;
		}
		return ret;
	}

}
