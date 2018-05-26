# ragingrapids

Finding paths quickly from one wiki article to another. Goal is to paths quickly, not necessarily efficiently.

This was something I built for an interview. Ended up not needing it, but it was fun anyway.

The techniques are solid, but it's overbuilt if you're just looking to solve the problem. I wanted to showoff a bit so I fleshed it out. It's a docker application that serves an api, uses a redis queue, uses muliple worker processes, caches results to quicker subsequent queries, and has a small frontend.
