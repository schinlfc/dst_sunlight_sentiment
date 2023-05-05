# Notes on crawler

These notes are a chronologically-ordered journal of notes about data decisions made on how to fetch data for the DST study. See README.md for consolidated, most recent documentation.

## 2022-05-28

Since the regression will eventually contain a dummy variable for each state, each tweet must be annotated with a state. Twitter provides a place_id attached to each geo-located tweet, so that place_id needs to be turned into a state.

I explored four approaches to this problem.

1. **Centroid**. I took the centroid field of each place, and used a state shapefile to map that latitude/longitude back into a state. This created a problem that some places might refer to a very broad geographic area, but their centroid will always be in a specific place. For example, see this [news story](https://splinternews.com/how-an-internet-mapping-glitch-turned-a-random-kansas-f-1793856052) for the problems this creates. This approach is artificially certain. I abandoned this approach.
2. **Bounding box**. I took the bounding box of the entire place, and matched that against my state shapefile. This had a lot of trouble, ironically on places such as "South Carolina, USA." The problem is that if you draw a rectangular bounding box around South Carolina, and check what states it intersects, it intersects many states besides South Carolina.

   I tried to solve this by introducing an 80% rule: if 80% of the intersection is in a single state, then resolve the point to that state. This solved some cases, but it didn't solve the South Carolina case.
3. **Name**. The next approach I tried was to use the name. There are only 50 states, so I can hard code a mapping of "South Carolina, USA" to "SC." (See state_names.json.) For cities, the name includes the state, so I wrote a regex to extract the state name from these places.
4. **Parent place.** Twitter places have a "contained_within" attribute. I tried using this to match poorly behaved places, but in cases where a place was hard to match, its parent place was even harder to match.

Here is the solution I settled on:

1. Try a place name match.
2. If that fails, try a geospatial match.
3. Consult an override file. If an override matches, prefer it over the other two rules.

### Example edge cases

Here's a sample of edge cases:

* [United States](https://twitter.com/places/96683cc9126741d1). Not possible to match this to state. Taking the centroid falsely reports it's in [Oklahoma](https://www.google.com/maps/place/36%C2%B053'27.2%22N+98%C2%B059'35.6%22W/@36.8908925,-98.9932214,17z/data=!3m1!4b1!4m5!3m4!1s0x0:0xa4a8790e4b0514ad!8m2!3d36.8908925!4d-98.9932214).
* [Washington Heights, Manhattan](https://twitter.com/places/046b03039470eeb2). Place name rule fails because the name doesn't include a state. The 80% rule fails because it's too close to the border. I hard-coded this exception.
* [Lincoln Tunnel](https://twitter.com/places/0fc295307d546000). This is a tunnel between New York and New Jersey, and jointly managed by both of them. Not sensible to say it's in either state. I dropped these.
* [Thomas A Edison Middle School](https://twitter.com/places/07d9cba210480003). This place has a bounding box which is only a single point, which causes the geospatial match to fail. I solved this by buffering the bounding box by 0.0001 degrees, about 10 meters, in cases where the bounding box had zero area.
* [Kansas City, MO](https://twitter.com/places/9a974dfc8efb32a0). Due to the closeness of the city to the border of Kansas, the geospatial match has trouble with this city. Issues with places like these were why I chose to do the place name match first and fall back to the city.

## 2022-06-01

Right now, we have the tweet date and time expressed in UTC. It would be much better if we had the tweet time for the local user - after all, we're interested in what time of day it is for them.

Two ideas:

1. Get timezone from [twitter user information](https://stackoverflow.com/questions/19013206/how-to-reliably-determine-time-zone-for-a-user-from-the-twitter-api).
2. Using lat/lon and [shapefile of timezones](https://github.com/evansiroky/timezone-boundary-builder), figure out what time zone they're in.

## 2022-06-02

It turns out Twitter [removed time zone info](https://stackoverflow.com/questions/50830126/getting-null-values-for-time-zone-and-utc-offset) from the API in 2018. Will have to figure out another approach.

## 2022-06-14

I've started writing code to determine which timezone a place falls into. To my surprise, 98% of the places which successfully geocode to a state also successfully geocode to exactly one timezone. A sophisticated approach is probably not required. I have a few ideas to catch the remaining places.

1. Take an average of different time zones if a place spans more than one time zone. If the time is UTC-6 in 40% of the place, and UTC-5 in the other 60% of the place, then compromise and say it's UTC-5:24.
2. Determine which time zones are synonyms for our purposes. E.g. Hurley, WI is detected as being in both Wisconsin and poking into the upper peninsula of Michigan. But since the upper peninsula has the same time zone as Wisconsin for all the years we care about, this is not an issue. Look into tzinfo for this.
3. Open Street Maps has a service called Nominatim which can be used to look up place names for a place. This would allow us to replace a bounding-box lookup with a more specific polygon lookup. Potentially also useful for state matching.  
