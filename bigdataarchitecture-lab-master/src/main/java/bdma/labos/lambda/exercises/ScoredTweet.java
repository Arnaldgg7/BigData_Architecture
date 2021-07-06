package bdma.labos.lambda.exercises;

import java.io.Serializable;

public class ScoredTweet implements Serializable {

	private Long id;
	private String text;
	private Float pos_score;
	private Float neg_score;
	private String type;

	public ScoredTweet(Long id, String text, Float pos_score, Float neg_score,
			String type) {
		super();
		this.id = id;
		this.text = text;
		this.pos_score = pos_score;
		this.neg_score = neg_score;
		this.type = type;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Float getPos_score() {
		return pos_score;
	}

	public void setPos_score(Float pos_score) {
		this.pos_score = pos_score;
	}

	public Float getNeg_score() {
		return neg_score;
	}

	public void setNeg_score(Float neg_score) {
		this.neg_score = neg_score;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
