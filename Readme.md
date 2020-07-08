# 介绍
godag是一款轻量级的DAG驱动引擎，包括以下功能：
1. 自由构建DAG图节点，需要在外部构建好以Node形式传递进去
2. DAG并行执行op
3. op执行过程中存储及传递结果
4. 支持op超时

# 同类产品对比
腾讯视频搜索有
1. go版本的引擎 https://git.code.oa.com/video_search_common/dag_np
2. 基于spp_rpc框架写的c++版本的引擎 https://git.code.oa.com/video_universal/dag_api/tree/master/
3. C++轻量级实现 https://git.code.oa.com/comp_video/dag_taskflow 【godag主要参考dag_taskflow实现】
开源的有
1. cpp-taskflow  https://github.com/cpp-taskflow/cpp-taskflow
2. transwarp       https://github.com/bloomen/transwarp

# 应用
1. SessionServer算子引擎  https://git.code.oa.com/video-fdmc/session_proxy_server