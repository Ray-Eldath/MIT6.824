# MIT6.824
MIT 6.824: Distributed Systems (Spring 2020)

- [X] [**Lab 1**](https://github.com/Ray-Eldath/MIT6.824/tree/mr)
- [X] [**Lab 2**](https://github.com/Ray-Eldath/MIT6.824/tree/raft-2021)
    - [X] Lab 2A
    - [X] Lab 2B
    - [X] Lab 2C
    - [X] Lab 2D
- [X] [**Lab 3**](https://github.com/Ray-Eldath/MIT6.824/tree/raft-2021)
    - [X] Lab 3A
    - [X] Lab 3B
- [X] [**Lab 4**](https://github.com/Ray-Eldath/MIT6.824/tree/raft-2021)
    - [X] Lab 4A
    - [X] Lab 4B
- [X] [**Lab 4 Challenge**](https://github.com/Ray-Eldath/MIT6.824/tree/optimized-2021)
    - [X] Challenge 1: Delete & Concurrent
    - [X] Challenge 2: Unaffected & Partial
- [X] [**Optimization: LeaseRead with noop**](https://github.com/Ray-Eldath/MIT6.824/tree/optimized-2021)
    - Now read-only requests could be served locally since it DOES NOT need to go through Raft layer anymore (
      implemented in kvraft and shardctler)

---

This repo is really a high quality implementation. All tests passed for at lease 1000 times, and Linearizable3B in kvraft passed for 10000 times. I finished two challenges in Lab 4 and a set of extra optimizations which are deemed hard to achieve by many (though a very small modification to test suites has been carried out). The code is well-structured and clean, leads to a well balance between maintainability and abstraction. Plus, I carefully cherry-picked and squashed commits to make them atomic, and only least required alternation is made which follows a clear logic.

I think the optimization (LeaseRead with noop) and several topics in shardkv are worth discussing, I'll write a blog post on this [on my personal blog](https://ray-eldath.me/) later on.

EDIT 2022/3/16: This blog post has been posted [here (in Chinese Simplified)](https://ray-eldath.me/programming/deep-dive-in-6824/).
