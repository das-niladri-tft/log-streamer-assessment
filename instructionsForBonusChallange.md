
## Bonus Challenges instruction

1. **Metrics endpoint**: Use `/metrics` to get:
   - Number of connected clients
   - Total lines streamed
   - Server uptime

2. **Line filtering**: Use `-pattern` flag then add the 'text' to get matching logs

3. **Multiple file support**: Use `-file file1.log,file2.log` comma seperated to get inital 10 lines from the each file and later the monitoring

4. **Better performance**: Use `-eventDriven true` make use of eventDriven endpints instead of polling