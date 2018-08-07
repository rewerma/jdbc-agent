<h1>功能介绍</h1>

<p style="font-size: 14px;">Jdbc-Agent用于代理与隔离应用的jdbc和物理数据库的连接，主要功能：</p>
<ol style="font-size: 14px;">
<li>隔离物理数据库，无需将数据库账号密码提供给开发者，开发者直接使用代理jdbc的账号密码连接</li>
<li>统一管理连接池，解决各个微服务连接池闲置等待及分布不均的问题</li>
<li>简化客户端的jdbc开发，客户端可以无需使用连接池来直连jdbc-agent server</li>
<li>server端高可用性，可以启动多台server，主server宕机能直接切换打备用server</li>
<li>可对sql调用日志统一管理</li>
<li>全局统一jdbc监控</li>
</ol>
