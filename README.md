# Computational graph stream service transformer

## Steps

1. **User defines computational graph**

    Developer uses our classes to express a computational graph

    1. Create `Node` classes
    2. Define computations for each `Node` class, if any
    3. Specify edges, connections between `Node`s
    4. Create `StartNode`s which will have special code to indicate where to pull data from
    5. Indicate `EndNode`s which will have special code to indicate where to push data to


2. **User selects streaming framework and deploys**

    Developer calls some function on the `ComputationalGraph` object they've created to deploy their system using a certain framework. The function signature should be something like the following:
    ```python
    def deploy_system(system_type: Type(SystemTypes)) -> int:
    ```

    With a usage like so:

    ```python
    my_cg = ComputationalGraph(nodes, start_nodes, end_nodes, edges)
    my_cg.deploy_system(SystemTypes.kafka)
    ```

    The work that actually needs to get done with this step is probably where the bulk of the work will be. The following section is a brainstorm area for this.
---

##  Deployment

We need to go from `ComputaionalGraph` to running nodes on different servers with connected parts. There may be different approaches considering different systems, however we're just going to focus on Kafka.

### Example
- We're given some computational graph: `my_cg`
- For simplicity's sake, let's say each node in `my_cg` gets one server (servers for now but could be a container)
- So if `my_cg` had 5 nodes, then we have to allocate resources and ask for 5 servers
- Assign a node to each server
- Consider the case of Node 1, assigned to let's say server 1
- Server 1 must start running the node code
- Thus, when running correctly, Node 1 is consuming from all subscribed topics, applying computations to them, and outputting to the correct topic

### Approach

It doesn't appear that we'll be touching much Kafka code at all. Instead, we just need to set up the environment which is apparently like so.

1. Start up the main zookeeper server
2. Spin up however many servers we want to be a part of the network

At this point we should have servers that are ready to become producers, consumers etc

3. Create all the topics we need (our edges in the computational graph)
4. Create producers for topics (this is where I get confused... where are we creating the producer and how can we customize the work it does. We want to create a producer which is going to run the custom code the user provides for that node)
5. Create consumers for topics (same concerns as above. In fact, we need to link the two together)
6. Connect the shit together? 

---

## Questions and TODOs
- When a user deploys the system, how will they interact with it from that point? I'm thinking of having some form of API where they can make queries to the system. Ideally we can query each node individaully for their status. Of particular interest would be to provide some interface to the last node so there is a way to look at results.



        
