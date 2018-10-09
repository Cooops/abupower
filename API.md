## Micro API Documentation
  _A fetching API for ABU Power and ABUR Duals, both the indexes themselves and the individual data points._

## URL

  `/api/<set>/<type>/<name>` - API endpoint for specific card data <br>
  `/api/<set>/<type>/table` -  API endpoint for general set data

## Method
  
  `GET` | ~~`POST`~~ | ~~`DELETE`~~ | ~~`PUT`~~
  
## URL Params

   **Required:**
 
   `set=[string]` - i.e. "alpha" or "beta" <br>
   `type=[string]` - i.e. "power" or "duals" <br>
   `name=[string]` - i.e. "mox jet" or "mox-jet"

   <!-- **Optional:**
 
   `photo_id=[alphanumeric]` -->

<!-- ## Data Params

  <_If making a post request, what should the body payload look like? URL Params rules apply here too._> -->

## Success Response
  
  * **Code:** 200 <br />
    **Content:** `{ results : [] }`
 
## Error Response

  <!-- <_Most endpoints will have many ways they can fail. From unauthorized access, to wrongful parameters etc. All of those should be listed here. It might seem repetitive, but it helps prevent assumptions from being made where they should be._> -->

  * **Code:** 404 Not Found <br />
    **Content:** `{ results : failed }, { error : errorMessage }`


## Sample `GET` Call

  The below calls are _very_ basic implementations. I would highly recommend wrapping them in functions with proper checks if you are going to be sending numerous requests.

  - JavaScript | AJAX & XHR
    ```
    var url = $.get(`https://abupower.com/api/beta/duals/table`)
    var response = url.responseJSON['results']
    ```
    &
    ```
    var data = JSON.stringify(false);

    var xhr = new XMLHttpRequest();
    xhr.withCredentials = true;

    xhr.addEventListener("readystatechange", function () {
      if (this.readyState === this.DONE) {
        console.log(this.responseText);
      }
    });

    xhr.open("GET", "https://abupower.com/api/unlimited/duals/tundra");

    xhr.send(data);
    ```
  - Python | Requests
    ```
    url = "/api/alpha/power/table"
    response = requests.get(url).json()
    results = response['results']
    ```
  - Linux | cURL
    ```
    curl --request GET --url https://abupower.com/api/alpha/power/table
    ```

## Notes

  I am happy to answer any questions or implement any improvements that are recommended or advised. Thanks for checking out my micro API documentation :). 
  <br><br>Last updated by Cooper Ribb @ 10/4/2018, 8:17AM CST. :blush:
