# :bar_chart: Project Progress and Tasks

## :label: Table of Contents

### Emoji Labels

- :red_circle: URGENT
- :yellow_circle: IMPORTANT
- :orange_circle: IN PROGRESS
- :green_circle: COMPLETED

---

## :heavy_check_mark: Evaluation Criteria (from rubric):

- Understanding of Kafka's background and significance (3 pts)
- Clear goals and objectives (3 pts)
- Document challenges encountered (3 pts)
- Successfully reproduce and extend Kafka concepts (3 pts)
- Clear explanation of concepts (3 pts)
- Professional writing and formatting (3 pts)
  **Total: 18 points**

---

## :pencil: General To Do List:

**Need to implement:**

- [ ] Reorganise Repo
- [ ] 3-broker cluster for fault tolerance
- [ ] Mutiple topics (data organization)
  - clicks
  - page views
  - searches
  - user sessions
- [ ] Multiple consumer groups (scalability)
- [ ] Stream processing (advanced features)
- [ ] Comprehensive testing
- [ ] Clear documentation
- [ ] Error handling
- [ ] Event validation
- [ ] Better code organization
- [ ] Create README with setup instructions
- [ ] Prepare all relevant diagrams
- [ ] Add comments to code + cleanup
- [ ] Final Report Created
- [ ] Events route to proper topics
- [ ] All testing goals completed, working, and documented
- [ ] Document all challenges faced

### Testing Goals

- [ ] Fault tolerance test (kill broker)
- [ ] Scalability test (partition count)
- [ ] Latency test (end-to-end timing)
- [ ] Load test (find bottlenecks)
- [ ] Document all results with graphs

---

## File specific To-dos

### `docker-compose.yml`

**Why:** We MUST showcase fault tolerance testing, which requires multiple brokers. With 1 broker, you can't test failover.

- [ ] NEEDS 3 brokers with replication

### Producer `kafka_producer.py`

**Why:** Current producer does not showcase Kafka capabilities. Need to show topic routing, partitioning strategy, and error resilience.

- [ ] Route events to appropriate topics based on type
- [ ] Add comprehensive error handling
- [ ] Track send metrics
- [ ] Use message keys for ordering

### Event handling

**Why:** Demonstrates Kafka's data organization, allows independent scaling

- [ ] Seperate topics by event type

### Consumer `kafka_consumer.py`

**Why:** Need to demonstrate consumer groups, parallel processing, and proper resource management.

- [ ] Add multiple consumer groups
- [ ] Add base consumer class for reusuability
- [ ] Implement graceful shutdown handling
- [ ] Implement better error handling

### Activities `activities.py`

**Why:** Current approach loses data on restart. Need to show understanding of stateful processing.

- [ ] Implement proper state management

### Main `main.py`

**Why:** Current code works but doesn't follow best practices. Improvements show professionalism (I wanna add this to my portfolio y'all).

- [ ] Separate endpoints per event type
- [ ] Use Pydantic validation
- [ ] Add WebSocket for real-time updates
- [ ] Add better lifecycle management

### Templates `index.html` and 'dashboard.html`

- [ ] Add tailwind CSS styling to make it more cunty

## Project Goals

- [ ] Architecture Analysis
  - Dive deep into the architecture of the chosen system. Understand its design principles, data flow mechanisms, and scalability. Document these findings, as they will form a crucial part of your final report.
- [ ] Functionalities and Features
  - Investigate the system's main functionalities and features, such as data storage methods, query optimization techniques, and security measures
- [ ] Case Study
  - [x] Developing a software application or app that utilizes the data management system for its data storage needs ✅ 2025-12-01
  - [ ] An extensive exploration of the system itself, where you employ and test various functionalities and features

1. Final Report
   1. This should provide an in-depth analysis of your project, including objectives, methodologies, results, and conclusions. Make sure it's well-structured and proofread.
2. Presentation and Demo (15 minutes)
   1. Conclude the project with a presentation that summarizes your in-depth analysis and case study. This presentation should include a demo showcasing either your software application, app, or extensive usage of the system. Highlight key insights and findings, and discuss any challenges you faced and how you overcame them.
   2. All group members must actively participate and contribute to the presentation. Record your presentation, upload it to a streaming platform such as Microsoft Stream or YouTube, and include the link in your submission
   3. Ensure your slides are clear, concise, and visually engaging, covering all the key points of your project

- p Project is peer reviewed based off of rubric

## Website List

- [x] Create Repo ✅ 2025-11-20
- [x] Create Index.html ✅ 2025-11-20
  - [ ] Can update to add more functionality
- [x] Create producer script ✅ 2025-11-20
  - [x] Connect to home page ✅ 2025-11-20
  - [ ] Can add more event listeners in index (for more types of data)
- [x] Actually download kafka + docker ✅ 2025-11-20
  - [ ] Sign up/in to docker
- [x] Create consumer script ✅ 2025-11-25
- [x] Create info displaying webpage ✅ 2025-11-25
- [ ] Update dashboard to look nicer
- [ ] Use docker to connect everything
  - [x] Used it to connect to kafka/zookeeper ✅ 2025-11-20
  - [ ] Maybe test replication factor for project? Param is in docker compose
