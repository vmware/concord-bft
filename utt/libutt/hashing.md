Regarding all these kinds of comments:

    // TODO(Crypto): See libutt/hashing.md

...if all our code will use the same hash function $H$, we should carefully create different hash functions for each use case by defining:

$$H_{use\_case\_name}(x) = H(use\_case\_name | x)$$

So these comments are meant to indicate we didn't do so and as a result cryptographic security problems might arise.
