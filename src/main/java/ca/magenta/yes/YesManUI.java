package ca.magenta.yes;

//import ca.magenta.yes.backend.Contact;
//import ca.magenta.yes.components.TimelineData;
import ca.magenta.utils.AppException;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.client.YesClient;
import ca.magenta.yes.data.NormalizedMsgRecord;
import ca.magenta.yes.ui.*;
import com.vaadin.server.FontAwesome;
import com.vaadin.server.VaadinRequest;
import com.vaadin.shared.ui.ValueChangeMode;
import com.vaadin.shared.ui.grid.HeightMode;
import com.vaadin.spring.annotation.SpringUI;
import com.vaadin.ui.*;
import com.vaadin.ui.themes.ValoTheme;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

/* User Interface written in Java.
 *
 * Define the user interface shown on the Vaadin generated web page by extending the UI class.
 * By default, a new UI instance is automatically created when the page is loaded. To reuse
 * the same instance, add @PreserveOnRefresh.
 */


@SpringUI
public class YesManUI extends UI {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(YesManUI.class.getPackage().getName());

    private YesClient yesClient = new YesClient("127.0.0.1",9595);

    private TimeRange timeRange = null;

    /*
    * Hundreds of widgets. Vaadin's user interface components are just Java
    * objects that encapsulate and handle cross-browser support and
    * client-server communication. The default Vaadin components are in the
    * com.vaadin.ui package and there are over 500 more in
    * vaadin.com/directory.
    */
    final TextField filter;
    final Grid eventList;
    //final Grid timeLineList;
    private final Button addNewBtn;
    private final Button showTimeline;
    private final Button showStructured;
    private final Button showRaw;

    // ContactForm is an example of a custom component class
    //final ContactForm contactForm;

    // ContactService is a in-memory mock DAO that mimics
    // a real-world datasource. Typically implemented for
    // example as EJB or Spring Data based service.
    //final ContactService service;

    private final CustomerRepository repo;

    private final CustomerEditor editor;

    private final ContactForm contactForm;

    //final Grid<Customer> grid;

    //final TextField filter;

    //VerticalLayout filterLayout = null;

    //private final Button addNewBtn;

    @Autowired
    public YesManUI(CustomerRepository repo, CustomerEditor editor) {

        try
        {
            timeRange = TimeRange.returnTimeRangeBackwardFromNow("last1y");
        }
        catch (AppException e)
        {
            e.printStackTrace();
        }

        // See https://vaadin.com/blog/-/blogs/using-vaadin-grid
        // See https://vaadin.com/docs/-/part/framework/datamodel/datamodel-providers.html

        this.repo = repo;
        this.editor = editor;

        this.filter = new TextField();
        //this.grid = new Grid<>(Customer.class);
        this.eventList = new Grid<>(NormalizedMsgRecord.class);
        //this.eventList = new Grid<>(Customer.class);
        this.addNewBtn = new Button("New record", FontAwesome.PLUS);

        // ContactForm is an example of a custom component class
        contactForm = new ContactForm();


        // Main window control
        this.showTimeline = new Button("T");
        this.showStructured = new Button("S");
        this.showRaw = new Button("R");

        // Timeline
        //this.timeLineList = new Grid<>(TimelineData.class);


    }

    /*
 * The "Main method".
 *
 * This is the entry point method executed to initialize and configure the
 * visible user interface. Executed on every browser reload because a new
 * instance is created for each web page loaded.
 */
    @Override
    protected void init(VaadinRequest request) {
        configureComponents();
        buildLayout();
    }

    private void configureComponents() {
        /*
         * Synchronous event handling.
         *
         * Receive user interaction events on the server-side. This allows you
         * to synchronously handle those events. Vaadin automatically sends only
         * the needed changes to the web page without loading a new page.
         */
//        newContact.addClickListener(e -> contactForm.edit(new Contact()));

//        filter.setCaption("Filter contacts...");
//        filter.addValueChangeListener((e -> refreshContacts(e.getValue())));

        //eventList.setContainerDataSource(new BeanItemContainer<>(Contact.class));

//        private long getRxTimestamp() {
//            return (long) data.get("rxTimestamp");
//        }
//
//    public String getPrettyRxTimestamp() {
//        return DATE_FORMAT.format(getRxTimestamp());
//    }
//
//    public String getPartition() {
//        return data.get("partition").toString();
//    }
//
//    public String getMessage() {
//
//    }
        filter.setPlaceholder("Filter by partition");
//        eventList.addColumn(getPrettyRxTimestamp()).setCaption("PrettyRxTimestamp");
//        eventList.addColumn("partition");
//        eventList.addColumn("message");
        eventList.setColumns("prettyRxTimestamp", "partition", "message");
        eventList.setColumnOrder("prettyRxTimestamp");
        //eventList.setColumns("rxTimestamp", "partition", "message");
        //eventList.setColumnOrder("rxTimestamp");
        //eventList.removeColumn("id");
        eventList.setSelectionMode(Grid.SelectionMode.SINGLE);
        filter.setValueChangeMode(ValueChangeMode.LAZY);
        filter.addValueChangeListener(e -> listCustomers(e.getValue()));
        //filter.addValueChangeListener(e -> listTimelines(e.getValue()));

        // Connect selected Customer to editor or hide if none is selected
        eventList.asSingleSelect().addValueChangeListener(e -> {
            editor.editCustomer((Customer) e.getValue());
        });

        // Instantiate and edit new Customer the new button is clicked
        //addNewBtn.addClickListener(e -> editor.editCustomer(new Customer("", "")));
        //addNewBtn.addClickListener(e -> editor.editCustomer(new Customer("", "")));
        addNewBtn.addClickListener(e -> contactForm.edit(new Contact()));

        // Listen changes made by the editor, refresh data from backend
        editor.setChangeHandler(() -> {
            editor.setVisible(false);
            listCustomers(filter.getValue());
        });

        // Initialize listing
        listCustomers(null);
//        eventList.addSelectionListener(
//                e -> contactForm.edit((Contact) eventList.getSelectedItems()));
//        refreshContacts();
   }

    /*
     * Robust layouts.
     *
     * Layouts are components that contain other components. HorizontalLayout
     * contains TextField and Button. It is wrapped with a Grid into
     * VerticalLayout for the left side of the screen. Allow user to resize the
     * components with a SplitPanel.
     *
     * In addition to programmatically building layout in Java, you may also
     * choose to setup layout declaratively with Vaadin Designer, CSS and HTML.
     */
    private void buildLayout() {

//        Button.ClickListener myClickListener = new Button.ClickListener() {
//            @Override
//            public void buttonClick(Button.ClickEvent event) {
//                String btCaption = event.getButton().getCaption();
//                myLabel.setValue(btCaption + " clicked");
//            }
//        };


        // https://examples.javacodegeeks.com/enterprise-java/vaadin/vaadin-button-example/
        // https://vaadin.com/forum#!/thread/9721905
        showTimeline.setSizeUndefined();
        showTimeline.addStyleName("tiny");
        showTimeline.setStyleName(ValoTheme.BUTTON_SMALL + " square");
        showStructured.setSizeUndefined();
        showStructured.addStyleName("tiny");
        showStructured.setStyleName(ValoTheme.BUTTON_SMALL + " square");
        showRaw.setSizeUndefined();
        showRaw.addStyleName("tiny");
        showRaw.setStyleName(ValoTheme.BUTTON_SMALL + " square");
        HorizontalLayout mainWindowControl = new HorizontalLayout(showTimeline, showStructured, showRaw);
        mainWindowControl.setMargin(false);
        mainWindowControl.setSpacing(false);

        HorizontalLayout actions = new HorizontalLayout(mainWindowControl, filter, addNewBtn);
        actions.setWidth("100%");
        filter.setWidth("100%");
        actions.setExpandRatio(filter, 1);



        VerticalLayout left = new VerticalLayout(actions, eventList);
        left.setSizeFull();
        eventList.setSizeFull();
        left.setExpandRatio(eventList, 1);

//        HorizontalLayout mainLayout = new HorizontalLayout(left, editor);
        HorizontalLayout mainLayout = new HorizontalLayout(left, contactForm);
        mainLayout.setSizeFull();
        mainLayout.setExpandRatio(left, 1);

        // Split and allow resizing
        setContent(mainLayout);

        // Initialize listing
        listCustomers(null);
    }

    // tag::listContacts[]
    public void listCustomers(String filterText) {
        if (StringUtils.isEmpty(filterText)) {

            logger.info(String.format("ROW COUNT: [%s]",Double.toString(eventList.getHeightByRows())));

            eventList.setItems(yesClient.findAll(timeRange,"*", false));
        }
        else {
            eventList.setHeightMode(HeightMode.ROW);
            logger.info(String.format("ROW COUNT: [%s]",Double.toString(eventList.getHeightByRows())));
            eventList.setItems(yesClient.findAll(timeRange,filterText, false));
        }
    }
    // end::listContacts[]

//    // tag::listTimelines[]
//    void listTimelines(String filterText) {
//        if (StringUtils.isEmpty(filterText)) {
//            timeLineList.setItems(repo.findAll());
//        }
//        else {
//            timeLineList.setItems(repo.findByLastNameStartsWithIgnoreCase(filterText));
//        }
//    }
//    // end::listTimelines[]



//    /*
//     * Choose the design patterns you like.
//     *
//     * It is good practice to have separate data access methods that handle the
//     * back-end access and/or the user interface updates. You can further split
//     * your code into classes to easier maintenance. With Vaadin you can follow
//     * MVC, MVP or any other design pattern you choose.
//     */
//    void refreshContacts() {
//        refreshContacts(filter.getValue());
//    }
//
//    private void refreshContacts(String stringFilter) {
//        eventList.setItems(service.findAll(stringFilter));
//        contactForm.setVisible(false);
//    }

//
//    @Override
//    protected void init(VaadinRequest request) {
//        // build layout
//        HorizontalLayout actions = new HorizontalLayout(filter, addNewBtn);
//        filterLayout = new VerticalLayout(actions);
//        filterLayout.setCaption("Filter");
//        filterLayout.setMargin(false);
//        VerticalLayout mainLayout = new VerticalLayout(filterLayout, grid, editor);
//        //mainLayout.setMargin(false);
//        mainLayout.setSizeFull();
//        setContent(mainLayout);
//        setSizeFull();
//
//
/////////////////
//
//        grid.setSizeFull();
//        grid.addStyleName(ValoTheme.TABLE_BORDERLESS);
//        grid.addStyleName(ValoTheme.TABLE_NO_HORIZONTAL_LINES);
//        grid.addStyleName(ValoTheme.TABLE_COMPACT);
//        grid.setSelectionMode(Grid.SelectionMode.SINGLE);
//
//        //grid.setColumnCollapsingAllowed(true);
//        //grid.setColumnCollapsible("time", false);
//        //grid.setColumnCollapsible("price", false);
//
//        grid.setColumnReorderingAllowed(true);
////        grid.setContainerDataSource(new TempTransactionsContainer(DashboardUI
////                .getDataProvider().getRecentTransactions(200)));
//        //grid.setSortContainerPropertyId("time");
//        //grid.setSortAscending(false);
//
//       // grid.setColumnAlignment("seats", Align.RIGHT);
//        //grid.setColumnAlignment("price", Align.RIGHT);
//
//        grid.setVisibleColumns("time", "country", "city", "theater", "room",
//                "title", "seats", "price");
//        grid.setColumnHeaders("Time", "Country", "City", "Theater", "Room",
//                "Title", "Seats", "Price");
//
//        grid.setFooterVisible(true);
//        grid.setColumnFooter("time", "Total");
//
//        grid.setColumnFooter(
//                "price",
//                "$"
//                        + DECIMALFORMAT.format(DashboardUI.getDataProvider()
//                        .getTotalSum()));
//
//        // Allow dragging items to the reports menu
//        grid.setDragMode(TableDragMode.MULTIROW);
//        grid.setMultiSelect(true);
//
//        grid.addActionHandler(new TransactionsActionHandler());
//
//        grid.addValueChangeListener(new ValueChangeListener() {
//            @Override
//            public void valueChange(final ValueChangeEvent event) {
//                if (table.getValue() instanceof Set) {
//                    Set<Object> val = (Set<Object>) table.getValue();
//                    createReport.setEnabled(val.size() > 0);
//                }
//            }
//        });
//        grid.setImmediate(true);
////////////////
//
//
//
//
//
//
//
//        //grid.setHeight(300, Unit.PIXELS);
//        grid.setColumns("id", "firstName", "lastName");
//
//        filter.setPlaceholder("Filter by last name");
//
//        // Hook logic to components
//
//        // Replace listing with filtered content when user changes filter
//        filter.setValueChangeMode(ValueChangeMode.LAZY);
//        filter.addValueChangeListener(e -> listCustomers(e.getValue()));
//
//        // Connect selected Customer to editor or hide if none is selected
//        grid.asSingleSelect().addValueChangeListener(e -> {
//            editor.editCustomer(e.getValue());
//        });
//
//        // Instantiate and edit new Customer the new button is clicked
//        //addNewBtn.addClickListener(e -> editor.editCustomer(new Customer("", "")));
//        addNewBtn.addClickListener(e -> this.addFilter());
//
//        // Listen changes made by the editor, refresh data from backend
//        editor.setChangeHandler(() -> {
//            editor.setVisible(false);
//            listCustomers(filter.getValue());
//        });
//
//        // Initialize listing
//        listCustomers(null);
//    }
//
//    // tag::listCustomers[]
//    void listCustomers(String filterText) {
//        if (StringUtils.isEmpty(filterText)) {
//            grid.setItems(repo.findAll());
//        }
//        else {
//            grid.setItems(repo.findByLastNameStartsWithIgnoreCase(filterText));
//        }
//    }
//    // end::listCustomers[]
//
//    public final void addFilter() {
//        filterLayout.addComponent(new HorizontalLayout(new TextField(), new Button("New customer", FontAwesome.PLUS)));
//    }


}