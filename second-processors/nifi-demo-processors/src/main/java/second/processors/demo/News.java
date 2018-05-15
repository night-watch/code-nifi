package second.processors.demo;

import java.io.Serializable;

/**
 * Created by NightWatch on 2018/1/12.
 */
public class News implements Serializable {

    private String id;

    private String title;

    private String action_type_x;

    private String image_url;

    private String type;

    private String tags;

    private Integer src;

    private String writer;

    private String url;

    private long read_count;

    private long comment_count;

    private long collection_count;

    private long datetime;

    @Override
    public String toString() {
        return "News{" +
                "id='" + id + '\'' +
                ", title='" + title + '\'' +
                ", action_type_x='" + action_type_x + '\'' +
                ", image_url='" + image_url + '\'' +
                ", type=" + type +
                ", tags=" + tags +
                ", src=" + src +
                ", writer='" + writer + '\'' +
                ", url='" + url + '\'' +
                ", read_count=" + read_count +
                ", comment_count=" + comment_count +
                ", collection_count=" + collection_count +
                ", datetime=" + datetime +
                '}';
    }

    public String getAction_type_x() {
        return action_type_x;
    }

    public void setAction_type_x(String action_type_x) {
        this.action_type_x = action_type_x;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getImage_url() {
        return image_url;
    }

    public void setImage_url(String image_url) {
        this.image_url = image_url;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public Integer getSrc() {
        return src;
    }

    public void setSrc(Integer src) {
        this.src = src;
    }

    public String getWriter() {
        return writer;
    }

    public void setWriter(String writer) {
        this.writer = writer;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getRead_count() {
        return read_count;
    }

    public void setRead_count(long read_count) {
        this.read_count = read_count;
    }

    public long getComment_count() {
        return comment_count;
    }

    public void setComment_count(long comment_count) {
        this.comment_count = comment_count;
    }

    public long getCollection_count() {
        return collection_count;
    }

    public void setCollection_count(long collection_count) {
        this.collection_count = collection_count;
    }

    public long getDatetime() {
        return datetime;
    }

    public void setDatetime(long datetime) {
        this.datetime = datetime;
    }

}
