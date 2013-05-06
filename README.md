Threadly
========

A library of java tools to assist with concurrent development.  There are tools to help with a wide range of development and testing.  This is designed to be a complement to java.util.concurrent and uses java.util.concurrent to help assist in it's implementations where it makes sense.

One of the key gems in this implementation is the thread pool's and thread pool tools which are provided.  The priority thread pool allows for faster throughput with multiple priorites.  Giving an option to provide more throughput and less strict run times on lower priority tasks.  While still allowing high priority tasks which will function like any other thread pool.

The TaskDistributor is an assistant in keeping operations on the same thread as much as possible.  It allows tasks to be scheduled with a key as to which thread they should run on.  Tasks with the same key will never run concurrently, but may NOT always run on the same thread.

The NoThreadScheduler is a scheduler which can be used in a more controlled way.  Helping with testing primarily it allows you to control how the thread pool advances.

The other huge benifit in this project is the ability to write fully concurrent code but be able to simply bring it under a single thread design for unit testing.  I will describe more on this later as it is finished.
