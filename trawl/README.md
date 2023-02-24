## `trawl`ing for books

As much as I'd prefer to have `crabs` doing my `trawl`ing, there are some boilerplate requirements to make use of the powerful `scrapy` library that would just be better not to hack into.  In that vein:

If, for some reason, you'd like to implement your own `scrapy` crawler, please use these instructions to get the basic setup complete.  Otherwise, the `shiprec` build should handle all of this without active consideration.

### installation
For complete documentation, please refer to [_the docs!_](https://docs.scrapy.org/en/latest/intro/install.html) 

#### on an Ubuntu-based system

1. On an Ubuntu-based system, install the required dependencies:
    ```bash
    sudo apt-get install python3 python3-dev python3-pip libxml2-dev libxslt1-dev zlib1g-dev libffi-dev libssl-dev
    ```

2. (In a recommended virtual environment of your choosing) Install `scrapy`:
    ```bash
    pip install scrap
    ```

#### on MacOS
have fun with `Xcode` and `PATH` settings and the `M1` chip compilation

#### on Windows
<img src="../img/wut.png"  width="125" height="75">

### setup

As always, [_the docs_.](https://docs.scrapy.org/en/latest/intro/tutorial.html#creating-a-project)

1. Navigate to your preferred directory, and substitute your preferred project name.  This will build the boilerplate settings needed to orchestrate `scrapy` functionality:
    ```bash
    scrapy startproject $projectname
    ```
2. Write/edit `spiders` to your hearts content
3. Given your `spiders` are constructed properly and the `name` attribute is set, then voila:
   ```bash
   scrapy crawl $spidername
   ```