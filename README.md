# Geektime 第六周作业

第六周 - 瞬间移动：构建强大高效的微服务

1. 为了使得query！正常运行，可能需要在crm命令行输入，DATABASE_URL更改为对应系统的URL：
Windows示例：
PS E:\06-crm-6-10-nginx-tls-interceptor\crm> 
$env:DATABASE_URL = "postgres://postgres:postgres@localhost:5432/stats"

# Run cargo sqlx prepare
cargo sqlx prepare

Mac示例：
PS E:\06-crm-6-10-nginx-tls-interceptor\crm> 
set DATABASE_URL = postgres://postgres:postgres@localhost:5432/stats

# Run cargo sqlx prepare
cargo sqlx prepare

2. 该数据库实现了以下功能：
1.recall 当用户若干天未登录平台时，系统需要根据用户的性别筛选出该性别用户观看量最多的视频，并从该用户的观看历史中找出与这些高观看量视频有交集的内容。按一定规则排序，并从中选择观看量最高的前九个作为推荐内容。推荐通知将通过应用内通知每天发送一次，而电子邮件通知则限制为每周发送一次。
Score(video, user) = w1 * views(video)+ w2 * gender_specific_views(video, user.gender)+ w3 * (likes(video) - dislikes(video))+ w4 * sum(similarity(user, other) * views(other, video) for all other)+ w5 * recent_views(video)- decay_factor(video)
2.remind 如果用户距离上次登录超过7天，系统将从用户的未观看完成列表中随机选取最多九个未完成的内容进行推荐。推荐通知将通过应用内通知每天发送一次，电子邮件通知则每周限发一次。

