# Listmonk syncing script

This script keeps attendee registration data in sync with our
listmonk instance.

When people register for the event they make a choice of what email they
want:

* Logistics about this event (not optional)
* Announcements (including future events) [optional]
* Sponsor offers [optional]

This takes that data and populates the various lists in listmonk.

## A note on unsubscribes

If people unsubscribe from a list, they still show up in the API as "on" that
list, so we never have to worry about accidentally adding someone to a list
they have unsubscribed from.
